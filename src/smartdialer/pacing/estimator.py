"""Interpretable answer-rate estimator.

Why not a gradient-boosted model or an LLM? Because the quantity we need is a
single probability whose *error bars* matter more than its point value, and
because a hiring panel (and, more importantly, a compliance reviewer) must be
able to read the number and reproduce it by hand. The estimator is deliberately
three lines of arithmetic:

    historical = (answers + k*p0) / (dials + k)     Bayesian (Beta) smoothing
    recent     = mean of the last W dial outcomes   reacts to change
    point      = w*recent + (1-w)*historical        blend

A sliding window rather than an exponential moving average: with Bernoulli
outcomes, an EMA fast enough to react usefully swings between 0 and 1 on every
observation, and we need a *stable* number plus an honest effective sample size
for the error bar. A window of the last W dials gives both, and "the answer
rate over the last 25 calls" is a sentence an operations manager can check.

Two extra outputs do the real safety work:

    stderr       sqrt(p(1-p)/n_eff)  -> how much we might be wrong by
    planning_rate = point + z*stderr -> what we *plan* against

Note the direction of the conservatism. High answer rates are the dangerous
side (more borrowers pick up than we have agents for), so we plan against the
*upper* bound of the answer rate. Dividing by a larger rate yields a smaller
number of dials.

``confidence`` collapses when the sample is small or when recent behaviour has
diverged from history. The Safety Controller reads it directly: an estimator
that has stopped understanding the world is not allowed to drive.

Replacing this with logistic regression or a calibrated classifier later means
implementing ``estimate()`` differently. Nothing downstream changes, because the
Safety Controller never trusts the estimate in the first place -- it treats it
as an input to a *cap*, not as permission.
"""

from __future__ import annotations

import math

from collections import deque

from ..config import PacingConfig
from ..models.domain import AnswerRateEstimate


class AnswerRateEstimator:
    def __init__(self, config: PacingConfig) -> None:
        self._config = config
        self._dials = 0
        self._answers = 0
        self._window: deque[float] = deque(maxlen=config.recent_window)
        self._talk_ema = config.default_talk_seconds
        self._talk_samples = 0

    # ------------------------------------------------------------ observation
    def observe_outcome(self, answered: bool) -> None:
        """One completed dial attempt. This is the only training signal."""
        self._dials += 1
        self._answers += 1 if answered else 0
        self._window.append(1.0 if answered else 0.0)

    def observe_talk_time(self, seconds: float) -> None:
        if seconds <= 0:
            return
        self._talk_samples += 1
        self._talk_ema = 0.8 * self._talk_ema + 0.2 * seconds

    # -------------------------------------------------------------- estimates
    @property
    def talk_seconds(self) -> float:
        return self._talk_ema

    @property
    def samples(self) -> int:
        return self._dials

    def estimate(self) -> AnswerRateEstimate:
        cfg = self._config
        k = cfg.prior_strength
        historical = (self._answers + k * cfg.prior_answer_rate) / (self._dials + k)
        window_n = len(self._window)
        if window_n >= cfg.min_recent_samples:
            recent = sum(self._window) / window_n
        else:
            recent = historical
        point = cfg.recent_weight * recent + (1 - cfg.recent_weight) * historical
        point = min(1.0, max(0.001, point))

        n_eff = self._dials + k
        stderr = math.sqrt(max(1e-9, point * (1 - point) / n_eff))
        planning_rate = min(1.0, point + cfg.confidence_z * stderr)

        sample_confidence = min(1.0, self._dials / max(1, cfg.samples_for_confidence))

        # Volatility is a z-score, not a raw difference: how many standard
        # errors the recent window sits away from the long-run rate. Random
        # noise gives z of about 1 and barely moves confidence; a genuine
        # regime change (70% collapsing to 10%) gives a very large z and drives
        # confidence to zero within one window, which is what makes the Safety
        # Controller fall back to progressive dialling.
        recent_se = math.sqrt(
            max(1e-9, recent * (1 - recent) / max(1, window_n))
        )
        z_divergence = abs(recent - historical) / max(recent_se, 0.02)
        volatility = min(1.0, z_divergence / cfg.regime_change_sigmas)
        confidence = max(0.0, sample_confidence * (1.0 - volatility))

        return AnswerRateEstimate(
            point=point,
            planning_rate=planning_rate,
            stderr=stderr,
            samples=self._dials,
            confidence=confidence,
            recent=recent,
            historical=historical,
            volatility=volatility,
        )
