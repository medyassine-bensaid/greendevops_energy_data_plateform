# bento_service.py — copy to project root, then: bentoml serve bento_service:svc
import bentoml
import numpy as np
from bentoml.io import NumpyNdarray, JSON

runner = bentoml.sklearn.get("energy_predictor:latest").to_runner()
svc    = bentoml.Service("energy_predictor_service", runners=[runner])

@svc.api(input=NumpyNdarray(), output=JSON())
async def predict(features: np.ndarray):
    pred = await runner.predict.async_run(features)
    rec  = "defer_2h" if pred[0] > 50 else ("defer_1h" if pred[0] > 20 else "run_now")
    return {"predicted_energy_j": float(pred[0]),
            "recommendation": rec,
            "confidence": "high" if abs(pred[0]) > 10 else "low"}