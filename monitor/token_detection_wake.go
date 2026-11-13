package monitor

func (m *Monitor) ensureTokenAssociationWake() chan struct{} {
	if m == nil {
		return nil
	}
	m.tokenAssociationWakeOnce.Do(func() { m.tokenAssociationWake = make(chan struct{}, 1) })
	return m.tokenAssociationWake
}
func (m *Monitor) signalTokenAssociationWake() {
	ch := m.ensureTokenAssociationWake()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// ensureTokenDetectionWake returns the process-local, coalescing notification
// used to wake the detector after a collection or association commit. The
// channel is deliberately never closed: shutdown is carried by the caller's
// context, so a late publisher cannot race a close and panic.
func (m *Monitor) ensureTokenDetectionWake() chan struct{} {
	if m == nil {
		return nil
	}
	m.tokenDetectionWakeOnce.Do(func() {
		m.tokenDetectionWake = make(chan struct{}, 1)
	})
	return m.tokenDetectionWake
}

// signalTokenDetectionWake coalesces any number of commits into one wake. A
// notification is only a scheduling hint; queue reads, leases, and all
// existing resource limits remain authoritative.
func (m *Monitor) signalTokenDetectionWake() {
	ch := m.ensureTokenDetectionWake()
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}
