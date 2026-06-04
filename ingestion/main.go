package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq"
)

// ─── Config ─────────────────────────────────────────────────────────────────

type Site struct {
	Name     string
	Host     string
	User     string
	RemotePath string
	Arch     string // x86_64 | aarch64
}

type Config struct {
	Sites        []Site
	LocalRawPath string
	PGConn       string
	Workers      int
	PollInterval time.Duration
}

func loadConfig() Config {
    return Config{
        LocalRawPath: envOr("RAW_PATH", "/data/raw"),
        PGConn:       envOr("PG_CONN", "host=localhost port=5432 user=greenops password=greenops dbname=greenops sslmode=disable"),
        Workers:      5,
        PollInterval: 5 * time.Minute,
        Sites: []Site{
            {Name: "rennes",  Host: envOr("RENNES_HOST", "frontend.rennes.grid5000.fr"),     User: "mbensaid", RemotePath: "/home/mbensaid/GreenDevOps_Restored/jobs_energy", Arch: "x86_64"},
            {Name: "lille",   Host: envOr("LILLE_HOST", "frontend.lille.grid5000.fr"),       User: "mbensaid", RemotePath: "/home/mbensaid/GreenDevOps_Restored/jobs_energy", Arch: "x86_64"},
            {Name: "nancy",   Host: envOr("NANCY_HOST", "frontend.nancy.grid5000.fr"),       User: "mbensaid", RemotePath: "/home/mbensaid/GreenDevOps_Restored/jobs_energy", Arch: "x86_64"},
            {Name: "lyon",    Host: envOr("LYON_HOST", "frontend.lyon.grid5000.fr"),         User: "mbensaid", RemotePath: "/home/mbensaid/GreenDevOps_Restored/jobs_energy", Arch: "x86_64"},
            {Name: "grenoble",Host: envOr("GRENOBLE_HOST", "frontend.grenoble.grid5000.fr"), User: "mbensaid", RemotePath: "/home/mbensaid/GreenDevOps_Restored/jobs_energy", Arch: "x86_64"},
        },
    }
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ─── Stats ───────────────────────────────────────────────────────────────────

var (
	filesCollected   uint64
	rowsIngested     uint64
	rowsRejected     uint64
	ingestionErrors  uint64
)

// ─── CSV Row ─────────────────────────────────────────────────────────────────

type EnergyRow struct {
	// Common
	Date            time.Time
	PipelineID      string
	CommitID        string
	RepoName        string
	ProjectName     string
	Category        string
	Branch          string
	Trigger         string
	// master_db specific
	JobName         string
	DurationS       float64
	CpuJ            float64
	RamJ            float64
	SdJ             float64
	NicJ            float64
	GpuJ            float64
	TotalEnergyJ    float64
	// granularity specific
	Component       string
	AvgPowerW       float64
	Samples         int
	// metadata
	Site            string
	Arch            string
	FileType        string // master | granularity | pipeline
	MeasurementSrc  string // process | ebpf | unknown
	SourceFile      string
	IngestedAt      time.Time
	IsValid         bool
	ValidationFlags string
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()

	db, err := sql.Open("postgres", cfg.PGConn)
	if err != nil {
		log.Fatalf("DB connect: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("DB ping: %v", err)
	}
	log.Println("✅ DB connected")

	go statsReporter()

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// First run immediately
	runIngestion(cfg, db)

	for range ticker.C {
		runIngestion(cfg, db)
	}
}

func runIngestion(cfg Config, db *sql.DB) {
	log.Printf("🔄 Ingestion cycle started — %d sites", len(cfg.Sites))
	var wg sync.WaitGroup
	sem := make(chan struct{}, cfg.Workers)

	for _, site := range cfg.Sites {
		wg.Add(1)
		sem <- struct{}{}
		go func(s Site) {
			defer wg.Done()
			defer func() { <-sem }()
			processSite(cfg, s, db)
		}(site)
	}
	wg.Wait()
	log.Printf("✅ Ingestion cycle done — collected=%d ingested=%d rejected=%d errors=%d",
		atomic.LoadUint64(&filesCollected),
		atomic.LoadUint64(&rowsIngested),
		atomic.LoadUint64(&rowsRejected),
		atomic.LoadUint64(&ingestionErrors))

	// Log to ingestion_log table
	logIngestionRun(db)
}

// ─── Per-site processing ─────────────────────────────────────────────────────

func processSite(cfg Config, site Site, db *sql.DB) {
	localSitePath := filepath.Join(cfg.LocalRawPath, site.Name)
	if err := os.MkdirAll(localSitePath, 0755); err != nil {
		log.Printf("❌ [%s] mkdir: %v", site.Name, err)
		return
	}

	// Rsync CSV files from remote node
	if err := rsyncSite(site, localSitePath); err != nil {
		log.Printf("❌ [%s] rsync: %v", site.Name, err)
		atomic.AddUint64(&ingestionErrors, 1)
		return
	}

	// Walk and ingest
	err := filepath.Walk(localSitePath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".csv") {
			return nil
		}
		// Skip lock files
		if strings.HasSuffix(path, ".lock") {
			return nil
		}
		atomic.AddUint64(&filesCollected, 1)
		fileType := detectFileType(path)
		rows := parseCSV(path, site, fileType)
		ingestRows(db, rows)
		return nil
	})
	if err != nil {
		log.Printf("❌ [%s] walk: %v", site.Name, err)
	}
}

func rsyncSite(site Site, localPath string) error {
	// Use rsync over SSH — requires SSH key auth set up on Grid5000
	// Pattern: rsync -avz --no-perms user@host:remote_path/ local_path/
	cmd := exec.Command("rsync",
		"-avz",
		"--no-perms",
		"--include=*.csv",
		"--include=*/",
		"--exclude=*",
		"--timeout=60",
		fmt.Sprintf("%s@%s:%s/", site.User, site.Host, site.RemotePath),
		localPath+"/",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("rsync failed: %v\n%s", err, string(out))
	}
	log.Printf("✅ [%s] rsync done", site.Name)
	return nil
}

// ─── File type detection ─────────────────────────────────────────────────────

func detectFileType(path string) string {
	base := filepath.Base(path)
	switch {
	case base == "master_energy_database.csv":
		return "master"
	case base == "pipeline_summary.csv":
		return "pipeline"
	case strings.HasPrefix(base, "history_"):
		return "granularity"
	}
	return "unknown"
}

func detectMeasurementSrc(headers []string) string {
	for _, h := range headers {
		if strings.ToLower(h) == "measurement" || strings.ToLower(h) == "measurement_source" {
			return "has_column" // actual value read per row
		}
	}
	return "unknown"
}

// ─── CSV Parsing ─────────────────────────────────────────────────────────────

func parseCSV(path string, site Site, fileType string) []EnergyRow {
	f, err := os.Open(path)
	if err != nil {
		log.Printf("❌ open %s: %v", path, err)
		return nil
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // allow variable

	headers, err := r.Read()
	if err != nil {
		return nil
	}
	// Normalize headers
	for i, h := range headers {
		headers[i] = snakeCase(h)
	}
	hIdx := headerIndex(headers)
	hasMeasCol := detectMeasurementSrc(headers)

	var rows []EnergyRow
	lineNum := 0
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		lineNum++

		row := EnergyRow{
			Site:       site.Name,
			Arch:       site.Arch,
			FileType:   fileType,
			SourceFile: path,
			IngestedAt: time.Now().UTC(),
		}

		// Date
		if i, ok := hIdx["date"]; ok && i < len(record) {
			row.Date = parseDate(record[i])
		}

		// Measurement source
		if hasMeasCol == "has_column" {
			for _, mkey := range []string{"measurement", "measurement_source"} {
				if i, ok := hIdx[mkey]; ok && i < len(record) {
					row.MeasurementSrc = normalizeMeasSrc(record[i])
					break
				}
			}
		} else {
			row.MeasurementSrc = "unknown"
		}

		// Common fields
		row.PipelineID = getStr(record, hIdx, "pipeline_id")
		row.CommitID   = getStr(record, hIdx, "commit_id")
		row.RepoName   = getStr(record, hIdx, "repo_name")
		row.ProjectName= normalizeProject(getStr(record, hIdx, "project_name"), row.RepoName)
		row.Category   = normalizeCategory(getStr(record, hIdx, "category"), row.RepoName)
		row.Branch     = getStr(record, hIdx, "branch")
		row.Trigger    = getStr(record, hIdx, "trigger")

		switch fileType {
		case "master":
			row.JobName    = getStr(record, hIdx, "job_name")
			row.DurationS  = getFloat(record, hIdx, "duration_s")
			row.CpuJ       = getFloat(record, hIdx, "cpu_j")
			row.RamJ       = getFloat(record, hIdx, "ram_j")
			row.SdJ        = getFloat(record, hIdx, "sd_j")
			row.NicJ       = getFloat(record, hIdx, "nic_j")
			row.GpuJ       = getFloat(record, hIdx, "gpu_j")
			row.TotalEnergyJ = getFloat(record, hIdx, "total_energy_j")

		case "granularity":
			// Extract job/component from filename: history_<job>_<component>.csv
			row.JobName, row.Component = extractJobComponent(path)
			row.DurationS = getFloat(record, hIdx, "duration_s")
			row.AvgPowerW = getFloat(record, hIdx, "avg_power_w")
			row.TotalEnergyJ = getFloat(record, hIdx, "total_energy_j")
			row.Samples   = int(getFloat(record, hIdx, "samples"))

		case "pipeline":
			row.TotalEnergyJ = getFloat(record, hIdx, "total_pipeline_energy_j")
		}

		row.IsValid, row.ValidationFlags = validate(row)
		rows = append(rows, row)
	}
	return rows
}

// ─── Validation ──────────────────────────────────────────────────────────────

func validate(row EnergyRow) (bool, string) {
	var flags []string

	if row.Date.IsZero() {
		flags = append(flags, "missing_date")
	}
	if row.PipelineID == "" {
		flags = append(flags, "missing_pipeline_id")
	}
	if row.TotalEnergyJ < 0 {
		flags = append(flags, "negative_energy")
	}
	if row.DurationS < 0 {
		flags = append(flags, "negative_duration")
	}
	// Suspect: zero total + non-zero duration (possible sensor failure)
	if row.TotalEnergyJ == 0 && row.DurationS > 10 {
		flags = append(flags, "zero_energy_suspect")
	}
	// Outlier guard: >1MJ for a single job is physically impossible on these nodes
	if row.TotalEnergyJ > 1_000_000 {
		flags = append(flags, "energy_outlier")
	}

	isValid := !contains(flags, "missing_date") &&
		!contains(flags, "negative_energy") &&
		!contains(flags, "negative_duration") &&
		!contains(flags, "energy_outlier")

	return isValid, strings.Join(flags, ",")
}

// ─── DB Write ────────────────────────────────────────────────────────────────

const insertMaster = `
INSERT INTO energy_jobs (
  date, pipeline_id, commit_id, repo_name, project_name, category,
  branch, trigger, job_name, duration_s, cpu_j, ram_j, sd_j, nic_j, gpu_j,
  total_energy_j, site, arch, measurement_src, source_file, ingested_at,
  is_valid, validation_flags
) VALUES (
  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23
)
ON CONFLICT (pipeline_id, job_name, site, date) DO NOTHING`

const insertGranularity = `
INSERT INTO energy_granularity (
  date, pipeline_id, commit_id, repo_name, project_name, category,
  branch, trigger, job_name, component, duration_s, avg_power_w,
  total_energy_j, samples, site, arch, measurement_src, source_file,
  ingested_at, is_valid, validation_flags
) VALUES (
  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
)
ON CONFLICT (pipeline_id, job_name, component, site, date) DO NOTHING`

const insertPipeline = `
INSERT INTO energy_pipelines (
  date, pipeline_id, commit_id, repo_name, project_name, category,
  branch, trigger, total_pipeline_energy_j, site, arch,
  measurement_src, source_file, ingested_at, is_valid, validation_flags
) VALUES (
  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
)
ON CONFLICT (pipeline_id, site) DO NOTHING`

func ingestRows(db *sql.DB, rows []EnergyRow) {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("❌ tx begin: %v", err)
		return
	}

	stmtM, _ := tx.Prepare(insertMaster)
	stmtG, _ := tx.Prepare(insertGranularity)
	stmtP, _ := tx.Prepare(insertPipeline)
	defer stmtM.Close()
	defer stmtG.Close()
	defer stmtP.Close()

	for _, row := range rows {
		var execErr error
		switch row.FileType {
		case "master":
			_, execErr = stmtM.Exec(
				row.Date, row.PipelineID, row.CommitID, row.RepoName, row.ProjectName,
				row.Category, row.Branch, row.Trigger, row.JobName, row.DurationS,
				row.CpuJ, row.RamJ, row.SdJ, row.NicJ, row.GpuJ, row.TotalEnergyJ,
				row.Site, row.Arch, row.MeasurementSrc, row.SourceFile, row.IngestedAt,
				row.IsValid, row.ValidationFlags,
			)
		case "granularity":
			_, execErr = stmtG.Exec(
				row.Date, row.PipelineID, row.CommitID, row.RepoName, row.ProjectName,
				row.Category, row.Branch, row.Trigger, row.JobName, row.Component,
				row.DurationS, row.AvgPowerW, row.TotalEnergyJ, row.Samples,
				row.Site, row.Arch, row.MeasurementSrc, row.SourceFile, row.IngestedAt,
				row.IsValid, row.ValidationFlags,
			)
		case "pipeline":
			_, execErr = stmtP.Exec(
				row.Date, row.PipelineID, row.CommitID, row.RepoName, row.ProjectName,
				row.Category, row.Branch, row.Trigger, row.TotalEnergyJ,
				row.Site, row.Arch, row.MeasurementSrc, row.SourceFile, row.IngestedAt,
				row.IsValid, row.ValidationFlags,
			)
		}
		if execErr != nil {
			atomic.AddUint64(&rowsRejected, 1)
		} else {
			atomic.AddUint64(&rowsIngested, 1)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("❌ tx commit: %v", err)
		tx.Rollback()
	}
}

func logIngestionRun(db *sql.DB) {
	_, err := db.Exec(`
		INSERT INTO ingestion_log (run_at, files_collected, rows_ingested, rows_rejected, errors)
		VALUES ($1, $2, $3, $4, $5)`,
		time.Now().UTC(),
		atomic.LoadUint64(&filesCollected),
		atomic.LoadUint64(&rowsIngested),
		atomic.LoadUint64(&rowsRejected),
		atomic.LoadUint64(&ingestionErrors),
	)
	if err != nil {
		log.Printf("❌ log run: %v", err)
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func snakeCase(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	return s
}

func headerIndex(headers []string) map[string]int {
	m := make(map[string]int, len(headers))
	for i, h := range headers {
		m[h] = i
	}
	return m
}

func getStr(record []string, idx map[string]int, key string) string {
	if i, ok := idx[key]; ok && i < len(record) {
		return strings.TrimSpace(record[i])
	}
	return ""
}

func getFloat(record []string, idx map[string]int, key string) float64 {
	s := getStr(record, idx, key)
	if s == "" {
		return 0
	}
	s = strings.ReplaceAll(s, ",", ".")
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func parseDate(s string) time.Time {
	s = strings.ReplaceAll(s, "_", " ")
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

func normalizeMeasSrc(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "ebpf", "bpf":
		return "ebpf"
	case "process", "proc":
		return "process"
	case "":
		return "unknown"
	}
	return "unknown"
}

func normalizeProject(project, repo string) string {
	if project != "" && project != "null" {
		return project
	}
	return strings.TrimPrefix(repo, "green_energy_org_")
}

func normalizeCategory(cat, repo string) string {
	if cat != "" && cat != "null" {
		return strings.ToUpper(cat)
	}
	// Infer from repo name
	r := strings.ToLower(repo)
	switch {
	case strings.Contains(r, "hpc"):
		return "HPC"
	case strings.Contains(r, "ml") || strings.Contains(r, "ai") || strings.Contains(r, "llm"):
		return "ML"
	case strings.Contains(r, "mlops") || strings.Contains(r, "devops"):
		return "MLOPS"
	case strings.Contains(r, "verl"):
		return "RL"
	}
	return "UNKNOWN"
}

func extractJobComponent(path string) (string, string) {
	base := strings.TrimSuffix(filepath.Base(path), ".csv") // history_docker-build_cpu
	parts := strings.SplitN(base, "_", 2)                   // ["history", "docker-build_cpu"]
	if len(parts) < 2 {
		return "unknown", "unknown"
	}
	rest := parts[1] // docker-build_cpu
	// Last underscore-separated token = component
	idx := strings.LastIndex(rest, "_")
	if idx < 0 {
		return rest, "unknown"
	}
	return rest[:idx], rest[idx+1:]
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func statsReporter() {
	t := time.NewTicker(30 * time.Second)
	for range t.C {
		log.Printf("📊 stats — files=%d ingested=%d rejected=%d errors=%d",
			atomic.LoadUint64(&filesCollected),
			atomic.LoadUint64(&rowsIngested),
			atomic.LoadUint64(&rowsRejected),
			atomic.LoadUint64(&ingestionErrors),
		)
	}
}
