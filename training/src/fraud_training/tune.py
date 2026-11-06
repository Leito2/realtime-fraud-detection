"""Bounded Optuna search for either library, on PR-AUC over the validation days (PLAN.md section 4.5)."""
import optuna

from fraud_training.metrics import pr_auc
from fraud_training.models import fit_model, native_scores


def _space(kind: str, trial: optuna.Trial) -> dict:
    common = {
        "learning_rate": trial.suggest_float("learning_rate", 0.03, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-2, 20.0, log=True),
    }
    if kind == "xgboost":
        return {**common, "max_depth": trial.suggest_int("max_depth", 3, 9),
                "min_child_weight": trial.suggest_float("min_child_weight", 1.0, 30.0, log=True)}
    return {**common, "num_leaves": trial.suggest_int("num_leaves", 15, 127),
            "min_child_samples": trial.suggest_int("min_child_samples", 10, 200, log=True)}


def tune(kind: str, Xtr, ytr, Xva, yva, n_trials: int, timeout_s: float, seed: int = 0) -> dict:
    """Best hyper-parameters found within ``n_trials`` or ``timeout_s``; ``{}`` when budget is zero."""
    if n_trials <= 0:
        return {}
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    def objective(trial: optuna.Trial) -> float:
        model = fit_model(kind, Xtr, ytr, Xva, yva, params=_space(kind, trial), seed=seed)
        return pr_auc(yva, native_scores(model, Xva))

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=seed))
    study.optimize(objective, n_trials=n_trials, timeout=timeout_s)
    return study.best_params
