use super::*;

#[test]
fn ack_at_or_below_the_stored_cursor_is_a_no_op() {
    let (state, publisher, published) = shareable();
    let note = entry(&state.database_id, &published, "photos", 1, 1);
    let state = apply(&state, &published, vec![note], 1);
    let state = ack(&state, &publisher, 1);
    let again = ack(&state, &publisher, 1);
    assert_eq!(again.acks.get(&publisher).copied(), Some(1));
    let lower = state.ack(&publisher, 0).expect("no-op");
    assert_eq!(lower.acks.get(&publisher).copied(), Some(1));
}

#[test]
fn ack_past_the_log_head_is_ahead() {
    let (state, publisher, published) = shareable();
    let note = entry(&state.database_id, &published, "photos", 1, 1);
    let state = apply(&state, &published, vec![note], 1);
    assert_eq!(state.ack(&publisher, 2).err(), Some(LogError::AckAhead));
}
