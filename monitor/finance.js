(function () {
  'use strict';

  const state = {
    inited: false, loaded: false, abort: null, chart: null,
    stale: false, refreshTimer: null, refreshAttempts: 0,
  };
  const $ = (id) => document.getElementById(id);
  const esc = (value) => String(value == null ? '' : value).replace(/[&<>"']/g, (char) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[char]));
  const statusText = {
    verified: '已核验', incomplete: '覆盖不完整', no_data: '暂无数据',
    paired_verified: '配对已核验', partially_paired: '部分配对',
    binding_required: '待完成来源归属', not_enrolled: '未进入配对账本', not_required: '暂不纳入',
    in_progress: '闭环进行中', bill_not_connected: '账单未接入',
    bill_missing: '账单证据缺失', correction_missing: '缺修正依据', source_binding_required: '成本来源待归属',
    cost_evidence_missing: '成本证据未采集', cost_evidence_incomplete: '成本证据未补齐', finance_history_missing: '历史财务版本缺失', ledger_backfill_required: '待试算配对账本',
    local_estimate_ready: '可闭合的本地估算', pricing_evidence_only: '仅倍率/费用证据', adapter_probe_required: '需适配器探测', range_exceeds_limit: '超过安全历史范围',
    account_missing: '账户配置缺失', account_disabled: '账户同步未启用', no_local_activity: '无本地活动依据', granularity_unsupported: '粒度不兼容', estimate_unavailable: '暂无估算',
    precheck_required: '待灰度前核对',
    not_connected: '未接入', not_configured: '未配置（不纳入）', disabled: '未开启',
  };
  const curProductNames = {
    AmazonECS: 'ECS / Fargate', AmazonRDS: 'RDS', AmazonCloudFront: 'CloudFront',
    CloudFrontPlans: 'CloudFront 套餐', AWSELB: 'ALB / ELB', AmazonLightsail: 'Lightsail',
    AmazonVPC: 'VPC / 公网网络', AmazonRoute53: 'Route 53', AmazonS3: 'S3',
    AmazonCloudWatch: 'CloudWatch', AWSSecretsManager: 'Secrets Manager',
    AmazonElastiCache: 'ElastiCache / Valkey', AmazonECR: 'ECR',
    AmazonApiGateway: 'API Gateway', awswaf: 'WAF', AWSLambda: 'Lambda',
    AWSDatabaseMigrationSvc: 'DMS', AmazonRegistrar: '域名注册',
    AmazonSNS: 'SNS', AWSCloudShell: 'CloudShell', AWSCostExplorer: 'Cost Explorer',
  };

  function money(value) {
    if (!value || value.micro_usd == null || value.micro_usd === '') return '—';
    try {
      let micro = BigInt(String(value.micro_usd));
      const sign = micro < 0n ? '-' : '';
      if (micro < 0n) micro = -micro;
      const whole = (micro / 1000000n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
      const fraction = (micro % 1000000n).toString().padStart(6, '0').slice(0, 2);
      return `${sign}$${whole}.${fraction}`;
    } catch (_) {
      return '—';
    }
  }

  function chartValue(value) {
    if (!value || value.micro_usd == null || value.micro_usd === '') return null;
    const numeric = Number(value.micro_usd);
    return Number.isFinite(numeric) ? numeric / 1000000 : null;
  }
  const date = (ts) => ts ? new Date(ts * 1000).toLocaleDateString('zh-CN', { timeZone: 'Asia/Shanghai' }) : '—';
  const dateTime = (ts) => ts ? new Date(ts * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '—';

  function userCoverage(value) {
    const expected = Number(value?.expected_hours || 0);
    const completed = Number(value?.completed_hours || 0);
    return expected ? `${completed}/${expected} 小时·${(completed * 100 / expected).toFixed(1)}%` : '—';
  }

  function upstreamCoverage(value) {
    const relevant = Number(value?.relevant_domains || 0);
    const available = Number(value?.available_domains || 0);
    const corrected = Number(value?.corrected_domains || 0);
    const unconfigured = Number(value?.unconfigured_domains || 0);
    const configured = relevant ? `账单 ${available}/${relevant}·修正 ${corrected}/${relevant}` : '无已配置上游';
    return unconfigured ? `${configured}·${unconfigured} 个未配置暂不纳入` : configured;
  }

  function domainHourCoverage(row) {
    const expected = Number(row?.expected_hours || 0);
    const completed = Number(row?.completed_hours || 0);
    return expected ? `${completed}/${expected} 小时·${(completed * 100 / expected).toFixed(1)}%` : '—';
  }

  function economicsCoverage(value) {
    const expected = Number(value?.expected_hours || 0);
    const verified = Number(value?.verified_hours || 0);
    return expected ? `${verified}/${expected} 域名小时` : '—';
  }

  function billBasis(value) {
    const parts = String(value || '').split('+').filter(Boolean).map((item) => ({
      hour: '小时账单', day: '自然日账单', closed_natural_days: '已闭合自然日',
    }[item] || item));
    return parts.length ? parts.join(' + ') : '—';
  }

  const chip = (status) => `<span class="fin-chip ${esc(status)}">${statusText[status] || esc(status)}</span>`;

  function evidenceBackfill(row) {
    const status = String(row?.evidence_backfill_status || '');
    if (!status) return '';
    const range = row.evidence_backfill_from && row.evidence_backfill_to
      ? `${date(row.evidence_backfill_from)} – ${date(row.evidence_backfill_to)}` : '';
    const calendarHours = Number(row.evidence_backfill_calendar_hours || 0);
    const coverage = row.evidence_backfill_granularity === 'natural_day'
      ? `${Number(calendarHours / 24).toLocaleString('zh-CN')} 个自然日 · ${Number(row.evidence_backfill_active_days || 0).toLocaleString('zh-CN')} 个活动日`
      : `${calendarHours.toLocaleString('zh-CN')} 日历小时 · ${Number(row.evidence_backfill_active_hours || 0).toLocaleString('zh-CN')} 活动小时`;
    const hasEstimate = status === 'local_estimate_ready' || status === 'pricing_evidence_only';
    const estimate = hasEstimate
      ? `${coverage} · 至少 ${Number(row.evidence_backfill_estimated_calls || 0).toLocaleString('zh-CN')} 次读取 / ${Number(row.evidence_backfill_estimated_runs || 0).toLocaleString('zh-CN')} 轮`
      : String(row.evidence_backfill_note || '');
    const caveat = hasEstimate ? String(row.evidence_backfill_note || '') : '';
    return `<span class="fin-backfill">${chip(status)}<small>${esc([range, estimate].filter(Boolean).join(' · '))}</small>${caveat ? `<small>${esc(caveat)}</small>` : ''}</span>`;
  }

  const hasMoney = (value) => value && value.micro_usd != null && value.micro_usd !== '';

  function knownValue(exact, known, partialText = '已知部分') {
    if (hasMoney(exact)) return money(exact);
    if (hasMoney(known)) return `${money(known)}<small>${partialText}</small>`;
    return '—';
  }

  function internalCostDeductionNote(statement) {
    return statement?.internal_cost_deduction_status
      ? '内部测试成本与已取得的上游成本尚未对齐；原成本保留，扣减后金额暂不发布'
      : '';
  }

  function grossCorrectedCost(statement) {
    return statement?.raw_corrected_upstream_cost || statement?.known_raw_corrected_upstream_cost || null;
  }

  function setMoney(id, exact, known, exactNote, partialNote, missingNote) {
    const element = $(id);
    if (!element) return;
    const exactAvailable = hasMoney(exact);
    const knownAvailable = hasMoney(known);
    const partial = !exactAvailable && knownAvailable;
    element.textContent = money(exactAvailable ? exact : knownAvailable ? known : null);
    element.classList.toggle('partial', partial);
    const note = $(`${id}Note`);
    if (note) note.textContent = !exactAvailable && !knownAvailable ? missingNote : partial ? partialNote : exactNote;
  }

  function setEvidence(id, completed, expected, unit) {
    const element = $(id);
    if (!element) return;
    const done = Number(completed || 0);
    const total = Number(expected || 0);
    const percent = total > 0 ? done * 100 / total : null;
    const value = element.querySelector('b');
    const note = element.querySelector('span');
    element.classList.remove('ready', 'warn', 'missing');
    element.classList.add(percent == null ? 'missing' : done >= total ? 'ready' : 'warn');
    if (value) value.textContent = percent == null ? '—' : `${percent.toFixed(1)}%`;
    if (note) note.textContent = total > 0
      ? `${done.toLocaleString('zh-CN')} / ${total.toLocaleString('zh-CN')} ${unit}`
      : '当前区间没有可核验证据';
  }

  function setGiftEvidence(value) {
    const element = $('finGiftEvidence');
    if (!element) return;
    const expected = Number(value?.expected_hours || 0);
    const userHours = Number(value?.user_completed_hours || 0);
    const creditHours = Number(value?.credit_completed_hours || 0);
    const completed = Math.min(userHours, creditHours);
    const boundaryExpected = Number(value?.expected_boundary_user_hours || 0);
    const boundaryCompleted = Number(value?.completed_boundary_user_hours || 0);
    const complete = value?.complete === true;
    const percent = expected > 0 ? completed * 100 / expected : null;
    element.classList.remove('ready', 'warn', 'missing');
    element.classList.add(complete ? 'ready' : percent == null ? 'missing' : 'warn');
    const amount = element.querySelector('b');
    const note = element.querySelector('span');
    // Hour coverage alone does not prove gift allocation and historical scope are complete.
    if (amount) amount.textContent = complete ? '已完成' : percent == null ? '—' : '待补齐';
    if (note) {
      const giftUsers = Number(value?.gift_users || 0);
      const sequence = boundaryExpected > 0 ? ` · 顺序 ${boundaryCompleted}/${boundaryExpected}` : '';
      const pending = value?.latest_hour_pending ? ' · 当前小时待闭合' : '';
      const scopePending = Number(value?.scope_unknown_events || 0) > 0 ? ` · ${Number(value.scope_unknown_events)} 条历史分组依据待补` : '';
      note.textContent = expected > 0
        ? `小时覆盖 ${percent.toFixed(1)}% · ${completed}/${expected} 小时 · 赠送用户 ${giftUsers}${sequence}${pending}${scopePending}`
        : '当前区间没有可核验证据';
    }
  }

  function setCUREvidence(value) {
    const element = $('finCUREvidence');
    if (!element) return;
    const enabled = value?.enabled === true;
    const loaded = value?.loaded === true;
    const coverage = Number(value?.coverage_ppm || 0) / 10000;
    const issues = Number(value?.issue_count || 0);
    const residual = value?.unallocated_cost?.display || money(value?.unallocated_cost);
    const conflicts = value?.conflict_cost?.display || money(value?.conflict_cost);
    element.classList.remove('ready', 'warn', 'missing');
    element.classList.add(!enabled || !loaded ? 'missing' : value?.range_complete ? 'ready' : 'warn');
    const amount = element.querySelector('b');
    const note = element.querySelector('span');
    if (amount) amount.textContent = !enabled ? '未启用' : !loaded ? '读取失败' : `${coverage.toFixed(4)}%`;
    if (note) note.textContent = !enabled ? 'CUR 产物未接入'
      : !loaded ? (value?.error || 'CUR 产物不可用')
        : value?.range_complete ? '当前区间成本证据已闭合'
          : `${value?.billing_finalized === false ? '当期未封账 · ' : ''}${value?.allocation_complete === false ? `待归属 ${residual}（${issues.toLocaleString('zh-CN')} 条）` : '归属已闭合'}${value?.conflict_nano_usd !== '0' ? ` · 冲突 ${conflicts}` : ''}`;
  }

  function operatingProfitBlockers(data, statement) {
    const blockers = [];
    if (!hasMoney(statement.operating_revenue)) {
      blockers.push(data.gift_coverage?.complete === true ? '经营收入未发布' : '注册赠送消耗证据未闭合');
    }
    if (!hasMoney(statement.raw_corrected_upstream_cost)) {
      const coverage = data.upstream_coverage || {};
      const relevant = Number(coverage.relevant_domains || 0);
      const available = Number(coverage.available_domains || 0);
      const corrected = Number(coverage.corrected_domains || 0);
      const details = Array.isArray(data.cost_details) ? data.cost_details : [];
      const billMissing = details.filter((row) => row.closure_readiness === 'bill_not_connected' || row.closure_readiness === 'bill_missing').length;
      const correctionMissing = details.filter((row) => row.closure_readiness === 'correction_missing').length;
      const unconfigured = Number(coverage.unconfigured_domains || 0);
      const reasons = [];
      if (billMissing) reasons.push(`${billMissing} 个上游账单未接入/缺失`);
      if (correctionMissing) reasons.push(`${correctionMissing} 个缺历史充值修正依据`);
      if (unconfigured) reasons.push(`${unconfigured} 个未配置来源暂不纳入正式毛利`);
      const coverageText = relevant ? `账单 ${available}/${relevant}、修正 ${corrected}/${relevant}` : '无可核验上游';
      blockers.push(`修正上游总成本未闭合（${coverageText}${reasons.length ? `；${reasons.join('、')}` : ''}）`);
    }
    if (!hasMoney(statement.aws_infrastructure_cost)) {
      const cur = data.cur_cost || {};
      if (cur.loaded !== true) blockers.push('AWS CUR 成本未载入');
      else if (cur.billing_finalized === false && cur.allocation_complete === false) blockers.push(`AWS 当期未封账且仍有 ${Number(cur.issue_count || 0).toLocaleString('zh-CN')} 条待归属`);
      else if (cur.billing_finalized === false) blockers.push('AWS 当期账单尚未封账');
      else if (cur.allocation_complete === false) blockers.push(`AWS 成本仍有 ${Number(cur.issue_count || 0).toLocaleString('zh-CN')} 条待归属`);
      else blockers.push('AWS 基础设施成本未发布');
    }
    return blockers;
  }

  function renderExecutive(data, statement) {
    const audit = data.pairing_audit || {};
    setEvidence('finUserEvidence', data.user_coverage?.completed_hours, data.user_coverage?.expected_hours, '小时');
    setGiftEvidence(data.gift_coverage);
    setEvidence('finBillEvidence', data.upstream_coverage?.available_domains, data.upstream_coverage?.relevant_domains, '上游');
    setEvidence('finCorrectionEvidence', data.upstream_coverage?.corrected_domains, data.upstream_coverage?.relevant_domains, '上游');
    setEvidence('finPairingEvidence', audit.paired_rows, audit.publication_rows, '核算行');
	setCUREvidence(data.cur_cost);

    const decision = $('finDecision');
    const badge = $('finDecisionBadge');
    const title = $('finDecisionTitle');
    const text = $('finDecisionText');
    const exactOperating = hasMoney(statement.operating_profit);
    const exactContribution = hasMoney(statement.contribution_profit);
    const knownContribution = hasMoney(statement.known_contribution_profit);
    const blockers = operatingProfitBlockers(data, statement);
    decision?.classList.remove('ready', 'warn');
    if (exactOperating) {
      decision?.classList.add('ready');
      if (badge) badge.textContent = '可发布';
      if (title) title.textContent = `当前区间经营毛利 ${money(statement.operating_profit)}`;
      if (text) text.textContent = '经营收入、修正上游成本和 AWS 基础设施成本均已闭合，可作为当前区间经营结论。';
      return;
    }
    decision?.classList.add('warn');
    if (exactContribution) {
      if (badge) badge.textContent = '渠道口径';
      if (title) title.textContent = '暂不发布全站经营毛利';
      if (text) text.textContent = `${blockers.join('；') || '全站经营证据未闭合'}。已核验计费贡献（赠送前）${money(statement.contribution_profit)}仅用于渠道分析，不是全站经营毛利。`;
      return;
    }
    if (badge) badge.textContent = '待核验';
    if (knownContribution) {
      if (title) title.textContent = '暂不发布全站经营毛利';
      if (text) text.textContent = `${blockers.join('；') || '全站经营证据未闭合'}。已配对计费贡献（赠送前）${money(statement.known_contribution_profit)}只覆盖已配对部分，不能与首屏金额直接相减。`;
      return;
    }
    if (title) title.textContent = '暂不发布全站利润';
    if (text) text.textContent = `${blockers.join('；') || '当前可发布证据未闭合'}。未覆盖金额不会默认当作 0，因此不能直接用首屏已知金额相减。`;
  }

  function setBridgeValue(id, value, known, exactLabel, partialLabel, missingLabel) {
    const amount = $(id);
    const stateLabel = $(`${id}State`);
    const exact = hasMoney(value);
    const partial = !exact && hasMoney(known);
    if (amount) {
      amount.textContent = money(exact ? value : partial ? known : null);
      amount.classList.toggle('partial', partial);
    }
    if (stateLabel) {
      stateLabel.textContent = exact ? exactLabel : partial ? partialLabel : missingLabel;
      stateLabel.className = exact ? 'ready' : partial ? 'warn' : 'missing';
    }
  }

  function renderProfitBridge(data, statement) {
    // Gross/refund fields are known amounts, not proof of complete coverage.
    // The backend's publishable net consumption supplies the completeness gate.
    const consumptionComplete=hasMoney(statement.user_consumption);
    for(const [id,field] of [['finBridgeGross','gross_user_consumption'],['finBridgeRefund','user_refunds']]) {
      setBridgeValue(id,consumptionComplete?statement[field]:null,statement[field],
        '完整','已知部分','无可核验金额');
    }
    setBridgeValue('finBridgeConsumption', statement.user_consumption, statement.known_user_consumption,
      '完整', '已知部分', '无可核验用量');
    setBridgeValue('finBridgeGift', statement.registration_gift_consumption, statement.known_registration_gift_consumption,
      '完整', '已知部分', '覆盖待补齐');
    setBridgeValue('finBridgeRevenue', statement.operating_revenue, statement.known_operating_revenue,
      '可发布', '仅已知部分', '待赠送扣减');
    setBridgeValue('finBridgeUpstream', statement.raw_corrected_upstream_cost, statement.known_raw_corrected_upstream_cost,
      '完整', '已知部分', '上游成本未闭合');
    setBridgeValue('finBridgeInternalTestCost', statement.internal_test_upstream_cost, statement.known_internal_test_upstream_cost,
      '完整', `严格识别 ${Number(statement.internal_test_cost_rows || 0).toLocaleString('zh-CN')} 小时`, '暂无可严格识别成本');
    setBridgeValue('finBridgeAWS', statement.aws_infrastructure_cost, statement.known_aws_infrastructure_cost,
      '完整', '已知部分', 'CUR 未接入');
    setBridgeValue('finBridgeProfit', statement.operating_profit, statement.known_operating_profit,
      '可发布', '仅已知部分', '收入或成本未闭合');
    const footnote = $('finBridgeFootnote');
    if (footnote) footnote.textContent = `平台经营结果扣除修正上游总成本（含内部测试支出），其中的内部账号、自动测试及非业务分组成本仅另列，不重复扣除。已配对客户成本 ${money(statement.paired_corrected_upstream_cost)}；内部账号与自动测试消费 ${money(statement.internal_test_consumption)}（${Number(statement.internal_test_requests || 0).toLocaleString('zh-CN')} 请求）不计客户收入。已严格识别非业务上游成本 ${money(statement.known_internal_test_upstream_cost)}；${Number(statement.internal_test_mixed_rows || 0).toLocaleString('zh-CN')} 个混合流量小时、${Number(statement.internal_test_unverified_pairs || 0).toLocaleString('zh-CN')} 个未核验对不做估算。`;
  }

  window.financeActivate = function () {
    if (!state.inited) init();
    if (!state.loaded || state.stale) load();
  };
  window.financeDeactivate = function () {
    if (state.abort) state.abort.abort();
    clearRefreshTimer();
  };

  function clearRefreshTimer() {
    if (state.refreshTimer) clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
  }

  function scheduleStaleRefresh() {
    clearRefreshTimer();
    if (!state.stale || $('tab-finance')?.hidden) return;
    // Fast snapshots intentionally skip the expensive fingerprint on this
    // request. Poll gently while the server verifies it in the background.
    if (String(state.cacheStatus || '').startsWith('fast-snapshot-')) {
      state.refreshTimer = setTimeout(() => load(false, true), 30000);
      return;
    }
    if (state.refreshAttempts >= 5) {
      const note = $('finCacheUpdate');
      if (note) note.textContent = ' · 更新尚未完成，已保留上次结果；请稍后刷新';
      return;
    }
    const delay = Math.min(16000, 1000 * (2 ** state.refreshAttempts));
    state.refreshAttempts += 1;
    state.refreshTimer = setTimeout(() => load(false, true), delay);
  }

  function init() {
    state.inited = true;
    $('finRefresh')?.addEventListener('click', () => load(true));
    $('finSinceLaunch')?.addEventListener('click', () => {
      $('finFrom').value = '';
      $('finTo').value = '';
      load();
    });
    $('finThisMonth')?.addEventListener('click', () => setMonth(0));
    $('finLastMonth')?.addEventListener('click', () => setMonth(-1));
    $('finInternalSave')?.addEventListener('click', saveInternalAccounts);
    loadInternalAccounts();
    $('finUnallocatedSourceRows')?.addEventListener('click', (event) => {
      const button = event.target.closest('[data-fin-cost-source]');
      if (!button) return;
      const domain = button.dataset.finDomain || '';
      const sourceRef = button.dataset.finCostSource || '';
      if (window.channelManagementOpenCostSource) window.channelManagementOpenCostSource(domain, sourceRef);
      else if (window.channelManagementOpen) window.channelManagementOpen({ domain });
    });
    window.addEventListener('resize', () => state.chart?.resize());
  }

  function internalSyncText(sync) {
    const status = String(sync?.status || 'not_configured');
    if (status === 'not_configured') return '未配置';
    if (status === 'caught_up') return '历史事实已补齐';
    if (status === 'error') return `同步异常：${sync?.last_error || '未知错误'}`;
    return `历史事实回填中 ${Number(sync?.progress_percent || 0).toFixed(1)}%`;
  }

  async function loadInternalAccounts() {
    const summary = $('finInternalSummary');
    const note = $('finInternalState');
    try {
      const response = await fetch('/finance/internal-accounts', { headers: { Accept: 'application/json' } });
      if (response.status === 401) { location.href = '/login'; return; }
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
      const rows = Array.isArray(data.accounts) ? data.accounts : [];
      if ($('finInternalAccounts')) $('finInternalAccounts').value = rows.map((row) => `${row.user_id}${row.username ? ` # ${row.username}` : ''}`).join('\n');
      if (summary) summary.textContent = `${rows.length} 个账号 · ${internalSyncText(data.sync)}`;
      if (note) note.textContent = rows.length ? `已按 user_id 固定身份；${internalSyncText(data.sync)}。混合流量小时不按请求数硬摊成本。` : '配置只保存在 Monitor，不修改 NewAPI 账号或原始日志。';
    } catch (error) {
      if (summary) summary.textContent = '配置读取失败';
      if (note) note.textContent = error.message;
    }
  }

  async function saveInternalAccounts() {
    const button = $('finInternalSave');
    const note = $('finInternalState');
    if (button) { button.disabled = true; button.textContent = '保存中'; }
    try {
      const raw = ($('finInternalAccounts')?.value || '').split(/\r?\n|,|，|;|；/).map((part) => part.trim()).filter(Boolean).map((part) => /^\d+\s+#\s+/.test(part) ? part.split(/\s+/)[0] : part).join('\n');
      const response = await fetch('/finance/internal-accounts', { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json' }, body: JSON.stringify({ accounts: raw }) });
      if (response.status === 401) { location.href = '/login'; return; }
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || data.error || `HTTP ${response.status}`);
      if (note) note.textContent = data.unchanged ? '配置未变更。' : '已保存；历史用量将在后台以只读方式回填，未补齐前报表不会假装成本已完整。';
      await loadInternalAccounts();
      await load(true);
    } catch (error) {
      if (note) note.textContent = `保存失败：${error.message}`;
    } finally {
      if (button) { button.disabled = false; button.textContent = '保存配置'; }
    }
  }

  function isoLocal(value) {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    }).formatToParts(value);
    const get = (type) => parts.find((part) => part.type === type)?.value || '';
    return `${get('year')}-${get('month')}-${get('day')}`;
  }

  function setMonth(delta) {
    const now = new Date();
    $('finFrom').value = isoLocal(new Date(now.getFullYear(), now.getMonth() + delta, 1));
    $('finTo').value = isoLocal(new Date(now.getFullYear(), now.getMonth() + delta + 1, 0));
    load();
  }

  async function load(forceFresh = false, background = false) {
    clearRefreshTimer();
    if (!background) state.refreshAttempts = 0;
    if (state.abort) state.abort.abort();
    state.abort = new AbortController();
    const button = $('finRefresh');
    if (button && !background) { button.disabled = true; button.textContent = '读取中'; }
    const query = new URLSearchParams();
    if ($('finFrom')?.value) query.set('from', $('finFrom').value);
    if ($('finTo')?.value) query.set('to', $('finTo').value);
    if (forceFresh) query.set('fresh', '1');
    try {
      const response = await fetch(`/finance/report${query.size ? `?${query}` : ''}`, {
        headers: { Accept: 'application/json' }, signal: state.abort.signal,
      });
      if (response.status === 401) { location.href = '/login'; return; }
      const data = await response.json();
      if (!response.ok) throw new Error(data.detail || data.error || `HTTP ${response.status}`);
      data._cache_status = response.headers.get('X-Monitor-Finance-Cache') || '';
      state.cacheStatus = data._cache_status;
      state.loaded = true;
      state.stale = String(data._cache_status).includes('stale');
      if (!state.stale) state.refreshAttempts = 0;
      render(data);
      scheduleStaleRefresh();
    } catch (error) {
      if (error.name !== 'AbortError') {
        if (background && state.stale) {
          const note = $('finCacheUpdate');
          if (note) note.textContent = ` · 更新失败，已保留上次结果：${String(error.message).slice(0, 160)}`;
          scheduleStaleRefresh();
        }
        else renderError(error.message);
      }
    } finally {
      if (button && !background) { button.disabled = false; button.textContent = '刷新'; }
    }
  }

  function financeIsPriorStale(data) {
    return String(data._cache_status || '').includes('prior-stale');
  }

  function financeCacheRefreshNote(data) {
    const status=String(data._cache_status || '');
    if (status.startsWith('fast-snapshot-')) {
      return '<span id="finCacheUpdate"> · 已核验快照；页面打开期间约每分钟核对事实版本，可点“刷新”立即核验</span>';
    }
    if (financeIsPriorStale(data)) {
      return `<span id="finCacheUpdate"> · 仅显示截至 ${dateTime(data.to)} 的较早区间，当前区间后台补算中；请勿当作当前结果</span>`;
    }
    return status.includes('stale')?'<span id="finCacheUpdate"> · 后台更新中</span>':'';
  }

  function render(data) {
    const enabled = data.enabled !== false;
    const statement = data.statement || {};
    const baseComplete = data.user_coverage?.complete && data.upstream_coverage?.complete;
    const giftComplete = data.gift_coverage?.complete === true;
    const complete = baseComplete && giftComplete;
    const priorStale = financeIsPriorStale(data);
    const status = $('finStatus');
    status.className = `fin-status ${!enabled ? 'bad' : !state.stale && complete ? 'ok' : ''}`;
    const summary = !enabled ? '经营核算功能尚未开启'
      : priorStale ? '较早区间快照，当前区间正在补算'
        : state.stale ? '已核验快照，正在核对最新事实'
        : complete ? '当前区间经营收入与上游成本证据完整'
        : baseComplete ? '用量与上游成本完整，注册赠送证据仍待补齐'
          : data.user_coverage?.complete ? '用户用量完整，部分上游成本仍待补证'
          : '已展示现有用量，部分小时与上游成本仍待补证';
    const snapshotNote=data.data_as_of?` · 快照截至 ${dateTime(data.data_as_of)}`:'';
    const generatedNote=data.generated_at?` · 生成于 ${dateTime(data.generated_at)}`:'';
    const refreshingNote=financeCacheRefreshNote(data);
    status.innerHTML = `<i></i><div><b>${summary}</b><br>${enabled ? `区间 ${date(data.from)} 至 ${date(data.to)} · 用量 ${userCoverage(data.user_coverage)} · 成本 ${upstreamCoverage(data.upstream_coverage)}${snapshotNote}${generatedNote}${refreshingNote}` : '配置 MONITOR_FINANCE_ENABLED=true 后，只读展示已有事实。'}</div>`;

    setMoney('finConsumption', statement.user_consumption, statement.known_user_consumption,
      `消费扣额 ${money(statement.gross_user_consumption)} - 退还 ${money(statement.user_refunds)}；内部测试另列 ${money(statement.internal_test_consumption)}`,
      `已知部分：扣额 ${money(statement.gross_user_consumption)} - 退还 ${money(statement.user_refunds)}；内部测试 ${money(statement.internal_test_consumption)}`,
      '当前区间没有可发布的用户消耗');
    setMoney('finBilled', statement.upstream_billed_cost, statement.known_upstream_billed_cost,
      '已覆盖所有相关上游账单', `已知部分 · ${upstreamCoverage(data.upstream_coverage)}`,
      '当前区间没有可发布的上游账单');
    setMoney('finUpstream', statement.corrected_upstream_cost, statement.known_corrected_upstream_cost,
      '使用当期充值到账/支付比例修正', `已知部分 · ${upstreamCoverage(data.upstream_coverage)}`,
      internalCostDeductionNote(statement) || '当前区间没有可发布的修正上游成本');
    setMoney('finContribution', statement.contribution_profit, statement.known_contribution_profit,
      `配对计费扣额 ${money(statement.paired_user_consumption)} - 配对成本 ${money(statement.paired_corrected_upstream_cost)}${statement.paired_contribution_margin_percent ? ` · ${statement.paired_contribution_margin_percent}%` : ''}；未扣注册赠送`,
      `已核验配对：${money(statement.paired_user_consumption)} - ${money(statement.paired_corrected_upstream_cost)}${statement.paired_contribution_margin_percent ? ` · ${statement.paired_contribution_margin_percent}%` : ''}；赠送前、非全站最终毛利`,
      '当前区间没有同时核验的收入与成本');

    renderExecutive(data, statement);
    renderProfitBridge(data, statement);
    renderCURProducts(data.cur_products || []);

    renderPeriods(data.periods || []);
    renderDays(data.days || []);
    renderCosts(data.cost_details || []);
    renderClosureReadiness(data.cost_details || []);
    renderEvidenceRollout(data.cost_details || []);
    renderPairingAudit(data.pairing_audit || {});
    renderSources(data.sources || []);
    renderChart(data.periods || []);
    $('finFootnote').textContent = data.semantics_note || '';
  }

  function renderCURProducts(rows) {
    const body = $('finCURProductRows');
    if (!body) return;
    body.innerHTML = rows.length ? rows.map((row) => {
      const code = String(row.product_code || '').trim();
      const name = curProductNames[code] || (code ? code : '未分类');
      const share = row.share_percent === '' || row.share_percent == null ? '—' : `${esc(row.share_percent)}%`;
      return `<tr><td><b>${esc(name)}</b></td>`
        + `<td><code>${esc(code || '未提供')}</code></td>`
        + `<td class="num">${knownValue(row.exact_cost, row.known_cost, '已知部分')}</td>`
        + `<td class="num">${share}</td>`
        + `<td class="num">${Number(row.included_days || 0).toLocaleString('zh-CN')}</td>`
        + `<td>${chip(row.status || 'incomplete')}</td></tr>`;
    }).join('') : '<tr><td colspan="6" class="fin-empty">当前区间没有完整自然日的 AWS 成本证据</td></tr>';
  }

  function renderPairingAudit(audit) {
    const rows = audit.blockers || [];
    const summary = $('finPairingSummary');
    const relevantDomains = Number(audit.relevant_domains || 0);
    const ledgerDomains = Array.isArray(audit.ledger_domains) ? audit.ledger_domains : [];
    const unenrolledDomains = Array.isArray(audit.unenrolled_domains) ? audit.unenrolled_domains : [];
    if (summary) summary.textContent = `${ledgerDomains.length}/${relevantDomains} 个上游已进入配对账本 · ${Number(audit.paired_rows || 0).toLocaleString('zh-CN')} 行已配对`;
    const coverage = $('finPairingCoverage');
    if (coverage) coverage.innerHTML = `<span><small>已进入不可变配对账本</small><b>${ledgerDomains.length ? ledgerDomains.map(esc).join('、') : '—'}</b></span>`
      + `<span class="${unenrolledDomains.length ? 'warn' : 'ready'}"><small>尚未进入配对账本</small><b title="${esc(unenrolledDomains.join('、'))}">${unenrolledDomains.length ? `${unenrolledDomains.length} 个：${unenrolledDomains.slice(0, 6).map(esc).join('、')}${unenrolledDomains.length > 6 ? '…' : ''}` : '无'}</b></span>`
      + `<span><small>待归属成本来源</small><b>${Number(audit.unallocated_sources || 0).toLocaleString('zh-CN')} 个</b></span>`;
    $('finPairingRows').innerHTML = rows.length ? rows.map((row) => {
      const domains = Array.isArray(row.domain_names) ? row.domain_names.filter(Boolean) : [];
      const domainCell = domains.length
        ? domains.map((domain) => `<b class="fin-domain-name">${esc(domain)}</b>`).join('')
        : `<span class="fin-muted">${Number(row.domains || 0).toLocaleString('zh-CN')} 个域名</span>`;
      const channelIDs=Array.isArray(row.channel_ids)?row.channel_ids.map(Number):[];
      const evidenceRange=row.first_hour?`${dateTime(row.first_hour)} – ${dateTime((row.last_hour||row.first_hour)+3600)}`:'时段未知';
      const channelLabel=channelIDs.length?channelIDs.map((id)=>id>0?`#${id}`:'未归属').join('、'):'渠道未知';
      return `<tr>`
      + `<td><b>${esc(row.name || row.key)}</b><small>${esc(row.key)}</small></td>`
      + `<td class="num">${Number(row.rows || 0).toLocaleString('zh-CN')}</td>`
      + `<td>${domainCell}</td>`
      + `<td class="num">${money(row.revenue)}</td>`
      + `<td class="num">${money(row.corrected_cost)}</td>`
      + `<td class="fin-gap-description">${esc(row.description || '—')}<small>${esc(evidenceRange)} · ${esc(channelLabel)}</small></td></tr>`;
    }).join('')
      : '<tr><td colspan="6" class="fin-empty">当前区间没有阻止毛利发布的已知缺口</td></tr>';
    renderPairingSources(audit);
  }

  function renderClosureReadiness(rows) {
    const target = $('finClosureReadiness');
    if (!target) return;
    const counts = rows.reduce((result, row) => {
      const key = row.closure_readiness || 'precheck_required';
      result[key] = (result[key] || 0) + 1;
      return result;
    }, {});
    const groups = [
      ['not_required', '暂不纳入', '未配置上游账户，不进入覆盖率和利润'],
      ['precheck_required', '待灰度前核对', '已有账单与修正证据'],
      ['correction_missing', '缺修正依据', '先补充值比例或审计证据'],
      ['bill_missing', '缺账单证据', '先补齐上游账单记录'],
      ['bill_not_connected', '账单未接入', '不能按零成本处理'],
      ['cost_evidence_missing', '成本证据未采集', '汇总账单不等于可配对小时证据'],
      ['cost_evidence_incomplete', '成本证据未补齐', '先补齐历史活动范围'],
      ['finance_history_missing', '历史财务版本缺失', '不用当前配置覆盖历史'],
      ['source_binding_required', '成本来源待归属', '需核对历史时段与本地渠道'],
      ['ledger_backfill_required', '待试算配对账本', '前置证据已具备'],
      ['in_progress', '闭环进行中', '继续补来源绑定与历史版本'],
      ['verified', '闭环已核验', '收入成本已经同窗闭合'],
    ].filter(([key]) => Number(counts[key] || 0) > 0);
    target.innerHTML = groups.length ? groups.map(([key, label, note]) =>
      `<span class="${esc(key)}"><small>${esc(label)}</small><b>${Number(counts[key] || 0).toLocaleString('zh-CN')} 个上游</b><em>${esc(note)}</em></span>`
    ).join('') : '<span><small>成本闭环准入</small><b>暂无相关上游</b></span>';
  }

  function renderPairingSources(audit) {
    const rows = Array.isArray(audit.sources) ? audit.sources : [];
    const summary = $('finUnallocatedSourceSummary');
    if (summary) summary.textContent = `${Number(audit.unallocated_sources || 0).toLocaleString('zh-CN')} 个待归属来源${audit.sources_truncated ? ' · 仅显示前 100 个' : ''}`;
    $('finUnallocatedSourceRows').innerHTML = rows.length ? rows.map((row) => {
      const sourceRef = String(row.source_ref || '');
      const groups = (Array.isArray(row.source_groups) ? row.source_groups : []).join('、') || '—';
      const models = (Array.isArray(row.upstream_models) ? row.upstream_models : []).join('、') || '—';
      const candidates = Array.isArray(row.current_config_candidates) ? row.current_config_candidates : [];
      const candidateLabel = candidates.length
        ? candidates.map((candidate) => `#${Number(candidate.channel_id || 0)} ${esc(candidate.channel_name || '未命名渠道')}`).join('<br>')
        : '—';
      const candidateNotes = {
        configured_unique: '仅分组名称得到一个候选；不代表令牌归属',
        configured_partial: '仅部分分组名称命中；不能直接归属',
        configured_ambiguous: '分组名称命中多个渠道；不能直接归属',
        no_configured_match: '当前配置未找到同名上游分组；不能据此判定未归属',
      };
      const inspectButton = row.domain && sourceRef
        ? `<button type="button" class="fin-inline-btn" data-fin-domain="${esc(row.domain)}" data-fin-cost-source="${esc(sourceRef)}">去渠道管理精确核对</button>`
        : '';
      return `<tr><td><b>${esc(row.domain || '—')}</b></td>`
        + `<td><code title="匿名化来源 SHA-256">${sourceRef ? `…${esc(sourceRef.slice(-10))}` : '—'}</code></td>`
        + `<td class="fin-source-dim" title="${esc(groups)}">${esc(groups)}</td>`
        + `<td class="fin-source-dim" title="${esc(models)}">${esc(models)}</td>`
        + `<td class="fin-source-candidate"><b>${candidateLabel}</b><small>${esc(candidateNotes[row.candidate_state] || '仅作人工核对提示，不影响核算')}</small>${inspectButton}</td>`
        + `<td>${date(row.first_hour)} – ${date(row.last_hour)}</td>`
        + `<td class="num">${Number(row.evidence_hours || 0).toLocaleString('zh-CN')}</td>`
        + `<td class="num">${Number(row.requests || 0).toLocaleString('zh-CN')}</td>`
        + `<td class="num">${money(row.billed_cost)}</td></tr>`;
    }).join('') : '<tr><td colspan="9" class="fin-empty">当前区间没有待归属的上游成本来源</td></tr>';
  }

  function renderPeriods(rows) {
    $('finPeriodRows').innerHTML = rows.length ? rows.slice().reverse().map((row) => {
      const statement = row.statement || {};
      return `<tr><td><b>${esc(row.period)}</b><small>${date(row.from)} – ${date(row.to)}</small></td>`
        + `<td class="num">${knownValue(statement.user_consumption, statement.known_user_consumption)}</td>`
        + `<td class="num">${knownValue(statement.registration_gift_consumption, statement.known_registration_gift_consumption)}</td>`
        + `<td class="num">${knownValue(statement.operating_revenue, statement.known_operating_revenue)}</td>`
        + `<td class="num">${knownValue(statement.upstream_billed_cost, statement.known_upstream_billed_cost)}</td>`
        + `<td class="num" title="含内部测试实际支出；右侧内部测试成本为其中项，不重复扣除">${knownValue(statement.raw_corrected_upstream_cost, statement.known_raw_corrected_upstream_cost)}</td>`
        + `<td class="num">${knownValue(statement.internal_test_upstream_cost, statement.known_internal_test_upstream_cost, '已识别')}</td>`
        + `<td class="num">${knownValue(statement.aws_infrastructure_cost, statement.known_aws_infrastructure_cost)}</td>`
        + `<td class="num">${knownValue(statement.contribution_profit, statement.known_contribution_profit, '已配对')}<small>${money(statement.paired_user_consumption)} - ${money(statement.paired_corrected_upstream_cost)}${statement.paired_contribution_margin_percent ? ` · ${statement.paired_contribution_margin_percent}%` : ''}</small></td>`
        + `<td>${userCoverage(row.user_coverage)}</td><td>${upstreamCoverage(row.upstream_coverage)}</td><td>${chip(row.status)}</td></tr>`;
    }).join('') : '<tr><td colspan="12" class="fin-empty">当前区间没有已发布的经营事实</td></tr>';
  }

  function dailyCorrectedCost(row) {
    const reconciliation = row.cost_reconciliation;
    const total = reconciliation?.status === 'reconciled' ? knownValue(null, reconciliation.known_total, '已取得') : '—';
    const note = reconciliation?.status === 'source_mismatch' || row.ledger_correction?.status === 'source_mismatch'
      ? '<small>日/月来源待核对</small>' : '';
    return `${total}${note}<small>充值 ${money(row.recharge_correction?.known_cost)} · 账本 ${money(row.ledger_correction?.known_cost)}</small>`;
  }

  function dailyInternalCost(row) {
    const statement = row.statement || {};
    const amount = knownValue(statement.internal_test_upstream_cost, statement.known_internal_test_upstream_cost, '已识别');
    const mixed = Number(statement.internal_test_mixed_rows || 0);
    const unverified = Number(statement.internal_test_unverified_pairs || 0);
    const notes = [];
    if (mixed > 0) notes.push(`混用待拆分 ${mixed.toLocaleString('zh-CN')} 项`);
    if (unverified > 0) notes.push(`未核验 ${unverified.toLocaleString('zh-CN')} 项`);
    const reasons = row.internal_cost_unverified_reasons || {};
    const labels = {
      channel_cost_not_published: '尚无渠道成本发布', upstream_cost_missing: '渠道上游成本缺失',
      hour_manifest_unverified: '小时账本未通过校验', cost_publication_unverified: '成本记录未通过校验',
      ambiguous_publication: '存在多份可用成本记录', unspecified: '历史原因待核对',
    };
    for (const [reason, count] of Object.entries(reasons).sort(([a], [b]) => a.localeCompare(b))) {
      if (Number(count) > 0) notes.push(`${labels[reason] || '其他原因待核对'} ${Number(count).toLocaleString('zh-CN')} 项`);
    }
    if (row.internal_cost_complete === false && !notes.length) notes.push('内部成本证据待补');
    if (row.internal_cost_complete === true && !hasMoney(statement.known_internal_test_upstream_cost)) notes.push('已核验，未发现内部测试成本');
    if (statement.internal_cost_deduction_status) notes.push('配对账本扣减待核对');
    return amount + (notes.length ? `<small>${notes.map(esc).join(' · ')}</small>` : '');
  }

  function renderDays(rows) {
    $('finDailyRows').innerHTML = rows.length ? rows.slice().reverse().map((row) => {
      const statement = row.statement || {};
      return `<tr><td><b>${esc(row.date)}</b></td>`
        + `<td class="num">${money(statement.gross_user_consumption)}</td>`
        + `<td class="num">${money(statement.user_refunds)}</td>`
        + `<td class="num">${knownValue(statement.user_consumption, statement.known_user_consumption)}</td>`
        + `<td class="num">${knownValue(statement.registration_gift_consumption, statement.known_registration_gift_consumption)}</td>`
        + `<td class="num">${knownValue(statement.operating_revenue, statement.known_operating_revenue)}</td>`
        + `<td class="num" title="上游账户原始账单，包含内部测试；不是已配对客户成本">${row.bill_coverage ? knownValue(statement.upstream_billed_cost, statement.known_upstream_billed_cost) : '—'}</td>`
        + `<td class="num" title="同月逐供应商核对后的已取得修正成本；账本与充值修正不重复，含内部测试，不代表全部成本或已配对客户成本">${dailyCorrectedCost(row)}</td>`
        + `<td class="num">${money(statement.internal_test_consumption)}<small>${Number(statement.internal_test_requests || 0).toLocaleString('zh-CN')} 请求</small></td>`
        + `<td class="num" title="内部成本单独核验；不能直接从旁边的账户成本相减，配对账本扣减仍需同源依据">${dailyInternalCost(row)}</td>`
        + `<td class="num">${knownValue(statement.aws_infrastructure_cost, statement.known_aws_infrastructure_cost)}</td>`
        + `<td class="num">${money(statement.paired_user_consumption)}</td>`
        + `<td class="num">${money(statement.paired_corrected_upstream_cost)}</td>`
        + `<td class="num">${knownValue(statement.contribution_profit, statement.known_contribution_profit, '已配对')}<small>${statement.paired_contribution_margin_percent ? `${statement.paired_contribution_margin_percent}%` : '—'}</small></td>`
        + `<td>${userCoverage(row.user_coverage)}</td><td>${economicsCoverage(row.economics_coverage)}</td><td>${chip(row.status)}</td></tr>`;
    }).join('') : '<tr><td colspan="17" class="fin-empty">当前区间没有每日经营事实</td></tr>';
  }

  function renderCosts(rows) {
    $('finCostRows').innerHTML = rows.length ? rows.map((row) => `<tr>`
      + `<td><b>${esc(row.domain)}</b></td>`
      + `<td class="num">${Number(row.user_requests || 0).toLocaleString('zh-CN')}</td>`
      + `<td class="num">${money(row.user_consumption)}</td>`
      + `<td class="num">${Number(row.upstream_requests || 0).toLocaleString('zh-CN')}</td>`
      + `<td class="num">${knownValue(row.billed_cost, row.known_billed_cost)}</td>`
      + `<td class="num">${knownValue(row.corrected_cost, row.known_corrected_cost)}</td>`
      + `<td class="num">${money(row.paired_revenue)}</td>`
      + `<td class="num">${money(row.paired_cost)}</td>`
      + `<td class="num">${knownValue(row.contribution, row.known_contribution, '已配对')}</td>`
      + `<td>${esc(billBasis(row.bill_basis))}</td>`
      + `<td>${esc(row.correction_source || '—')}</td>`
      + `<td>${domainHourCoverage(row)}${row.data_until ? `<small>截至 ${date(row.data_until)}</small>` : ''}</td>`
      + `<td>${chip(row.status)}</td>`
      + `<td>${chip(row.pairing_status || 'not_enrolled')}</td>`
      + `<td class="fin-closure-action">${chip(row.closure_readiness || 'precheck_required')}<small>${esc(row.closure_next_action || '尚未完成准入判断')}</small>${evidenceBackfill(row)}</td></tr>`).join('')
      : '<tr><td colspan="15" class="fin-empty">暂无可核对的上游成本明细</td></tr>';
  }

  function renderEvidenceRollout(rows) {
    const target = $('finEvidenceRollout');
    const body = $('finEvidenceRolloutRows');
    const summary = $('finEvidenceRolloutSummary');
    if (!target || !body || !summary) return;
    const candidates = rows.filter((row) => row.evidence_backfill_status);
    const closable = candidates.filter((row) => row.evidence_backfill_status === 'local_estimate_ready' && row.evidence_backfill_closes_cost === true)
      .sort((a, b) => Number(a.evidence_backfill_estimated_calls || 0) - Number(b.evidence_backfill_estimated_calls || 0) || String(a.domain).localeCompare(String(b.domain)));
    const pricingOnly = candidates.filter((row) => row.evidence_backfill_status === 'pricing_evidence_only');
    const blocked = candidates.length - closable.length - pricingOnly.length;
    const calls = closable.reduce((sum, row) => sum + Number(row.evidence_backfill_estimated_calls || 0), 0);
    const runs = closable.reduce((sum, row) => sum + Number(row.evidence_backfill_estimated_runs || 0), 0);
    const pilot = closable[0];
    summary.textContent = closable.length
      ? `${closable.length} 个可闭合候选 · 至少 ${calls.toLocaleString('zh-CN')} 次读取 / ${runs.toLocaleString('zh-CN')} 轮`
      : '当前没有可直接闭合的候选';
    target.innerHTML = `<span class="${pilot ? 'ready' : 'warn'}"><small>建议首个只读灰度</small><b>${pilot ? esc(pilot.domain) : '—'}</b><em>${pilot ? `NewAPI · 至少 ${Number(pilot.evidence_backfill_estimated_calls || 0).toLocaleString('zh-CN')} 次读取` : '先解决账号、粒度或历史范围阻断'}</em></span>`
      + `<span><small>可生成渠道成本账本</small><b>${closable.length} 个</b><em>仅 NewAPI 逐请求证据</em></span>`
      + `<span class="${pricingOnly.length ? 'warn' : 'ready'}"><small>仅可补倍率/费用证据</small><b>${pricingOnly.length} 个</b><em>Sub2API / AICodeWith 暂不解除渠道毛利阻断</em></span>`
      + `<span class="${blocked ? 'warn' : 'ready'}"><small>尚未可估算</small><b>${blocked} 个</b><em>需补账号、粒度或分段方案</em></span>`;
    body.innerHTML = candidates.length ? candidates.slice().sort((a, b) => {
      const rank = (row) => row.evidence_backfill_closes_cost === true ? 0 : row.evidence_backfill_status === 'pricing_evidence_only' ? 1 : 2;
      return rank(a) - rank(b) || Number(a.evidence_backfill_estimated_calls || Number.MAX_SAFE_INTEGER) - Number(b.evidence_backfill_estimated_calls || Number.MAX_SAFE_INTEGER) || String(a.domain).localeCompare(String(b.domain));
    }).map((row) => {
      const isPilot = pilot && row.domain === pilot.domain;
      const granularity = row.evidence_backfill_granularity === 'natural_day' ? '自然日' : row.evidence_backfill_granularity === 'hour' ? '小时' : '—';
      const impact = hasMoney(row.corrected_cost) ? money(row.corrected_cost) : money(row.known_corrected_cost);
      const work = Number(row.evidence_backfill_estimated_calls || 0) > 0
        ? `${Number(row.evidence_backfill_estimated_calls).toLocaleString('zh-CN')} 次 / ${Number(row.evidence_backfill_estimated_runs || 0).toLocaleString('zh-CN')} 轮`
        : '—';
      const result = row.evidence_backfill_closes_cost === true ? '可进入渠道成本闭环' : row.evidence_backfill_status === 'pricing_evidence_only' ? '仅改善上游证据' : '尚不可执行';
      return `<tr><td><b>${esc(row.domain || '—')}</b>${isPilot ? '<small class="fin-pilot">建议首个灰度</small>' : ''}</td>`
        + `<td>${esc(row.provider_name || row.provider || '—')}</td><td>${esc(granularity)}</td>`
        + `<td class="num">${impact}</td><td class="num">${esc(work)}</td><td>${esc(result)}</td>`
        + `<td>${chip(row.evidence_backfill_status)}<small>${esc(row.evidence_backfill_note || '')}</small></td></tr>`;
    }).join('') : '<tr><td colspan="7" class="fin-empty">当前没有需要补采的历史成本证据</td></tr>';
  }

  function renderSources(rows) {
    $('finSources').innerHTML = rows.map((row) => `<div class="fin-source"><div><b>${esc(row.name)}</b>${chip(row.status)}</div><p>${esc(row.description)}</p></div>`).join('');
  }

  function renderChart(rows) {
    const element = $('finChart');
    if (!element || !window.echarts) return;
    if (!state.chart) state.chart = echarts.init(element);
    const periods = rows.map((row) => row.period);
    const pick = (statement, exact, known) => statement?.[exact] || statement?.[known];
    const consumption = rows.map((row) => chartValue(pick(row.statement, 'user_consumption', 'known_user_consumption')));
    const billed = rows.map((row) => chartValue(pick(row.statement, 'upstream_billed_cost', 'known_upstream_billed_cost')));
    const corrected = rows.map((row) => chartValue(grossCorrectedCost(row.statement)));
    const contribution = rows.map((row) => chartValue(pick(row.statement, 'contribution_profit', 'known_contribution_profit')));
    const hasData = [...consumption, ...billed, ...corrected, ...contribution].some((value) => value != null);
    state.chart.setOption({
      backgroundColor: 'transparent', animationDuration: 250,
      grid: { left: 62, right: 20, top: 34, bottom: 42 },
      tooltip: { trigger: 'axis', valueFormatter: (value) => value == null ? '—' : `$${Number(value).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` },
      legend: { top: 1, textStyle: { color: '#9aa5b8', fontSize: 10 }, data: ['净计费消耗', '账单原值', '修正上游总成本', '已配对计费贡献（赠送前）'] },
      xAxis: { type: 'category', data: periods, axisLine: { lineStyle: { color: '#394153' } }, axisLabel: { color: '#8f9aac' } },
      yAxis: { type: 'value', axisLine: { show: false }, splitLine: { lineStyle: { color: '#2a3140' } }, axisLabel: { color: '#8f9aac', formatter: (value) => `$${value}` } },
      graphic: hasData ? [] : [{ type: 'text', left: 'center', top: '47%', style: { text: '当前区间暂无已发布金额', fill: '#7f8a9e', font: '12px sans-serif', textAlign: 'center' } }],
      series: [
        { name: '净计费消耗', type: 'bar', data: consumption, itemStyle: { color: '#d99b42' }, barMaxWidth: 21 },
        { name: '账单原值', type: 'bar', data: billed, itemStyle: { color: '#77859d' }, barMaxWidth: 21 },
        { name: '修正上游总成本', type: 'bar', data: corrected, itemStyle: { color: '#55b995' }, barMaxWidth: 21 },
        { name: '已配对计费贡献（赠送前）', type: 'line', data: contribution, connectNulls: false, symbolSize: 6, lineStyle: { width: 2, color: '#72a8fa', type: 'dashed' }, itemStyle: { color: '#72a8fa' } },
      ],
    });
    setTimeout(() => state.chart?.resize(), 60);
  }

  function renderError(message) {
    const status = $('finStatus');
    status.className = 'fin-status bad';
    status.innerHTML = `<i></i><div><b>经营核算读取失败</b><br>${esc(message)}</div>`;
    $('finPeriodRows').innerHTML = '<tr><td colspan="12" class="fin-empty">请稍后刷新；失败不会被解释为 0</td></tr>';
    $('finDailyRows').innerHTML = '<tr><td colspan="17" class="fin-empty">请稍后刷新；失败不会被解释为 0</td></tr>';
    $('finPairingRows').innerHTML = '<tr><td colspan="6" class="fin-empty">请稍后刷新；缺口不会被忽略</td></tr>';
    $('finCURProductRows').innerHTML = '<tr><td colspan="6" class="fin-empty">请稍后刷新；AWS 成本不会被解释为 0</td></tr>';
  }
}());
