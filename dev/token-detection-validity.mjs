// Audit the same dependency tuple used by tokenDetectionStaleIdentities.
// Fact/link equality alone is not proof that a stored head is current.
export const tokenHeadColumns = `c.policy_hash AS detected_policy_hash,
 c.reference_generation AS detected_reference_generation,COALESCE(b.generation,0) AS current_reference_generation,
 c.source_complete AS detected_source_complete,
 CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END AS current_source_complete`;
export const tokenHeadJoins = ` LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket
 LEFT JOIN token_metering_windows w ON w.source_epoch=c.source_epoch AND w.start_at=(c.created_at/300)*300 `;
export function tokenHeadStaleReasons(row, policy) {
 const reasons=[];
 if(!row.detection_state)reasons.push('head_missing');
 else {
  if(row.detected_fact_revision!==row.revision)reasons.push('fact_revision_changed');
  if(row.detected_link_revision!==row.link_revision)reasons.push('link_revision_changed');
  if(policy && row.detected_policy_hash!==policy)reasons.push('policy_changed');
  if((row.detected_reference_generation??-1)!==row.current_reference_generation)reasons.push('reference_generation_changed');
  if((row.detected_source_complete??-1)!==row.current_source_complete)reasons.push('source_window_changed');
 }
 return reasons;
}
