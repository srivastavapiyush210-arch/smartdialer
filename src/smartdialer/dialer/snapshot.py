"""One consistent read of the world, shared by pacing and safety.

Both read the *same* object, so a pacing log line and the matching safety line
describe the same instant and can be compared directly.
"""

from __future__ import annotations

from ..metrics.collector import M, MetricsCollector
from ..models.domain import SystemSnapshot
from ..models.enums import AgentState
from ..repositories.agents import AgentRepository
from ..repositories.borrowers import BorrowerRepository
from ..repositories.calls import CallRepository


class SnapshotBuilder:
    def __init__(
        self,
        agents: AgentRepository,
        borrowers: BorrowerRepository,
        calls: CallRepository,
        metrics: MetricsCollector,
        clock,
    ) -> None:
        self._agents = agents
        self._borrowers = borrowers
        self._calls = calls
        self._metrics = metrics
        self._clock = clock

    async def build(
        self, campaign_id: str, *, provider_health: float, circuit_state: str
    ) -> SystemSnapshot:
        agent_counts = await self._agents.counts_by_state(campaign_id)
        call_counts = await self._calls.live_counts(campaign_id)
        eligible = await self._borrowers.count_eligible(campaign_id)
        recent_dials, recent_answers, recent_abandoned = \
            self._metrics.recent_outcomes()

        snapshot = SystemSnapshot(
            at=self._clock.now(),
            campaign_id=campaign_id,
            available_agents=agent_counts[AgentState.AVAILABLE],
            reserved_agents=agent_counts[AgentState.RESERVED],
            dialing_agents=agent_counts[AgentState.DIALING],
            connected_agents=agent_counts[AgentState.CONNECTED],
            wrap_up_agents=agent_counts[AgentState.WRAP_UP],
            paused_agents=agent_counts[AgentState.PAUSED],
            offline_agents=agent_counts[AgentState.OFFLINE],
            unbound_calls_in_flight=call_counts["unbound_in_flight"],
            bound_calls_in_flight=call_counts["bound_in_flight"],
            ringing_calls=call_counts["ringing"],
            answered_awaiting_agent=call_counts["answered_awaiting_agent"],
            connected_calls=call_counts["connected"],
            eligible_borrowers=eligible,
            provider_health=provider_health,
            circuit_state=circuit_state,
            answers_observed=int(self._metrics.counter(M.CALLS_ANSWERED)),
            abandoned_observed=int(self._metrics.counter(M.CALLS_ABANDONED)),
            dials_observed=int(self._metrics.counter(M.CALLS_INITIATED)),
            recent_dials=recent_dials,
            recent_answers=recent_answers,
            recent_abandoned=recent_abandoned,
        )
        self._publish(snapshot)
        return snapshot

    def _publish(self, snapshot: SystemSnapshot) -> None:
        m = self._metrics
        m.gauge(M.G_AVAILABLE_AGENTS, snapshot.available_agents)
        m.gauge(M.G_RESERVED_AGENTS, snapshot.reserved_agents)
        m.gauge(M.G_CONNECTED_AGENTS, snapshot.connected_agents)
        m.gauge(M.G_WRAP_UP_AGENTS, snapshot.wrap_up_agents)
        m.gauge(M.G_OFFLINE_AGENTS, snapshot.offline_agents)
        m.gauge(M.G_RINGING_CALLS, snapshot.ringing_calls)
        m.gauge(M.G_UNBOUND_IN_FLIGHT, snapshot.unbound_calls_in_flight)
        m.gauge(M.G_PROVIDER_HEALTH, snapshot.provider_health)
