export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return withCors(new Response(null, { status: 204 }));
    }

    try {
      if (url.pathname === "/" && request.method === "GET") {
        return withCors(htmlResponse(renderAppHtml()));
      }

      if (url.pathname === "/api/health" && request.method === "GET") {
        return withCors(json({ ok: true, mode: "manual-scenario-v2" }));
      }

      if (url.pathname === "/api/events/manual" && request.method === "GET") {
        const rows = await supabaseSelect(env, "events", "id,trigger_type_id,zone_cible,date_evenement,occurs_at,predicted_start_at,predicted_end_at,payload,statut,dedupe_key", {
          statut: "eq.open",
          order: "id.desc",
          limit: 100,
        });
        const triggerTypes = await supabaseSelect(env, "trigger_types", "id,code,label", { order: "id.asc" });
        const typeMap = new Map(triggerTypes.map((t) => [t.id, t]));
        return withCors(json(rows.map((r) => ({ ...r, trigger_type: typeMap.get(r.trigger_type_id) || null }))));
      }

      if (url.pathname === "/api/scenarios" && request.method === "GET") {
        const scenarios = await supabaseSelect(env, "scenarios", "id,code,label,trigger_type_id,aggregation_mode,priority,active", {
          active: "eq.true",
          order: "priority.desc",
        });
        return withCors(json(scenarios));
      }

      if (url.pathname.startsWith("/api/scenarios/") && url.pathname.endsWith("/steps") && request.method === "GET") {
        const scenarioId = Number(url.pathname.split("/")[3]);
        const steps = await supabaseSelect(env, "scenario_steps", "id,scenario_id,code,step_order,window_ref,window_min_hours,window_max_hours,logic_json,active", {
          scenario_id: `eq.${scenarioId}`,
          active: "eq.true",
          order: "step_order.asc",
        });
        return withCors(json(steps));
      }

      if (url.pathname === "/api/manual-launch" && request.method === "POST") {
        const body = await request.json();
        const result = await launchScenarioForEvent(env, body);
        return withCors(json(result));
      }

      if (url.pathname === "/api/jobs" && request.method === "GET") {
        const jobs = await supabaseSelect(env, "client_message_items", "id,client_id,event_id,scenario_id,scenario_step_id,planned_send_at,priority,subject_rendered,status,created_at,sent_at", {
          order: "planned_send_at.asc",
          limit: 200,
        });
        return withCors(json(jobs));
      }

      if (url.pathname === "/api/process-due" && request.method === "POST") {
        const result = await processDueMessages(env);
        return withCors(json(result));
      }

      return withCors(json({ error: "Route introuvable" }, 404));
    } catch (error) {
      return withCors(json({ error: error.message || "Erreur interne" }, 500));
    }
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(processDueMessages(env));
  },
};

async function launchScenarioForEvent(env, payload) {
  const { event_id, scenario_id, dry_run = false, trigger_send_immediately = false } = payload || {};
  if (!event_id || !scenario_id) throw new Error("event_id et scenario_id sont obligatoires");

  const [event] = await supabaseSelect(env, "events", "*", { id: `eq.${event_id}`, limit: 1 });
  if (!event) throw new Error("Événement introuvable");

  const [scenario] = await supabaseSelect(env, "scenarios", "*", { id: `eq.${scenario_id}`, limit: 1, active: "eq.true" });
  if (!scenario) throw new Error("Scénario introuvable ou inactif");

  const steps = await supabaseSelect(env, "scenario_steps", "*", {
    scenario_id: `eq.${scenario.id}`,
    active: "eq.true",
    order: "step_order.asc",
  });
  if (!steps.length) throw new Error("Aucune étape active sur ce scénario");

  const clients = await supabaseSelect(env, "clients", "id,email,zone_geo,preferences,active,siret", {
    active: "eq.true",
    zone_geo: `eq.${event.zone_cible}`,
  });

  const contentItems = await supabaseSelect(env, "content_items", "*", { active: "eq.true" });
  const contentVersions = await supabaseSelect(env, "content_versions", "*", {
    status: "eq.published",
    order: "version_no.desc",
  });

  const itemByCode = new Map(contentItems.map((x) => [x.code, x]));
  const latestVersionByContentId = new Map();
  for (const v of contentVersions) {
    if (!latestVersionByContentId.has(v.content_item_id)) {
      latestVersionByContentId.set(v.content_item_id, v);
    }
  }

  const now = new Date();
  const preview = [];
  let created = 0;

  for (const client of clients) {
    let previousPlannedAt = now;

    for (let index = 0; index < steps.length; index++) {
      const step = steps[index];
      const logic = step.logic_json || {};
      const rules = Array.isArray(logic.contents) ? logic.contents : [];
      const delayHoursAfterPrevious = Number(logic.delay_hours_after_previous ?? (index === 0 ? 0 : 0));
      const plannedAt = new Date(previousPlannedAt.getTime() + delayHoursAfterPrevious * 3600 * 1000);
      previousPlannedAt = plannedAt;

      const renderContext = buildRenderContext(event, client, step, scenario);
      const renderedBlocks = [];
      const appliedVersions = [];
      let firstSubject = "";

      for (const rule of rules) {
        const contentItem = itemByCode.get(rule.content_code);
        if (!contentItem) continue;
        const version = latestVersionByContentId.get(contentItem.id);
        if (!version) continue;

        const subject = renderTemplate(version.sujet_template || "", renderContext);
        const body = renderTemplate(version.corps_template || "", renderContext);
        if (!firstSubject && subject) firstSubject = subject;
        if (body) renderedBlocks.push(body);
        appliedVersions.push({ content_code: rule.content_code, content_version_id: version.id });
      }

      if (!renderedBlocks.length) continue;

      const subjectRendered = firstSubject || `[Prévention routière] ${scenario.label} - ${step.code}`;
      const bodyRendered = renderedBlocks.join("\n\n");

      preview.push({
        client_id: client.id,
        client_email: client.email,
        event_id: event.id,
        scenario_id: scenario.id,
        scenario_step_id: step.id,
        planned_send_at: plannedAt.toISOString(),
        subject_rendered: subjectRendered,
      });

      if (!dry_run) {
        await supabaseInsert(env, "client_message_items", {
          client_id: client.id,
          event_id: event.id,
          scenario_id: scenario.id,
          scenario_step_id: step.id,
          planned_send_at: plannedAt.toISOString(),
          priority: scenario.priority || 50,
          subject_rendered: subjectRendered,
          body_rendered: bodyRendered,
          render_context: renderContext,
          applied_content_versions: appliedVersions,
          cooldown_key: `manual:${event.id}:${scenario.id}:${step.id}:${client.id}`,
          status: trigger_send_immediately && delayHoursAfterPrevious === 0 ? "ready" : "ready",
          sent_at: null,
        });
        created++;
      }
    }
  }

  if (!dry_run && trigger_send_immediately) {
    await processDueMessages(env);
  }

  return {
    ok: true,
    mode: dry_run ? "simulation" : "execution",
    event_id,
    scenario_id,
    clients_concernes: clients.length,
    messages_programmes: dry_run ? preview.length : created,
    send_immediately: !!trigger_send_immediately,
    preview,
  };
}

async function processDueMessages(env) {
  const nowIso = new Date().toISOString();
  const dueItems = await supabaseSelect(env, "client_message_items", "id,client_id,event_id,scenario_id,scenario_step_id,planned_send_at,priority,subject_rendered,body_rendered,render_context,applied_content_versions,status", {
    status: "eq.ready",
    planned_send_at: `lte.${nowIso}`,
    order: "planned_send_at.asc",
    limit: 200,
  });

  if (!dueItems.length) {
    return { ok: true, processed: 0, sent: 0 };
  }

  const clients = await supabaseSelect(env, "clients", "id,email,zone_geo,siret", { active: "eq.true" });
  const clientMap = new Map(clients.map((c) => [c.id, c]));

  let sent = 0;

  for (const item of dueItems) {
    const client = clientMap.get(item.client_id);
    if (!client?.email) continue;

    const outbound = await supabaseInsert(env, "outbound_emails", {
      client_id: item.client_id,
      send_date: item.planned_send_at.slice(0, 10),
      planned_send_at: item.planned_send_at,
      subject_rendered: item.subject_rendered,
      body_rendered: item.body_rendered,
      status: "queued",
      presta_id: null,
      sent_at: null,
    });

    await supabaseInsert(env, "outbound_email_items", {
      outbound_email_id: outbound.id,
      client_message_item_id: item.id,
      display_order: 1,
    });

    const sendResult = await sendEmail(env, {
      to: client.email,
      subject: item.subject_rendered,
      html: item.body_rendered.replace(/\n/g, "<br>"),
    });

    await supabasePatch(env, "outbound_emails", outbound.id, {
      status: "sent",
      presta_id: sendResult.provider_id || "mail-provider",
      sent_at: new Date().toISOString(),
    });

    await supabasePatch(env, "client_message_items", item.id, {
      status: "sent",
      sent_at: new Date().toISOString(),
    });

    await supabaseInsert(env, "envois_log", {
      outbound_email_id: outbound.id,
      client_id: item.client_id,
      event_id: item.event_id,
      sent_at: new Date().toISOString(),
      presta_id: sendResult.provider_id || "mail-provider",
      message: item.body_rendered,
    });

    sent++;
  }

  return {
    ok: true,
    processed: dueItems.length,
    sent,
  };
}

function buildRenderContext(event, client, step, scenario) {
  return {
    ...(event.payload || {}),
    zone_cible: event.zone_cible,
    date_evenement: event.date_evenement,
    occurs_at: event.occurs_at,
    predicted_start_at: event.predicted_start_at,
    predicted_end_at: event.predicted_end_at,
    client_email: client.email,
    client_zone_geo: client.zone_geo,
    client_siret: client.siret,
    scenario_code: scenario.code,
    scenario_label: scenario.label,
    step_code: step.code,
  };
}

function renderTemplate(template, context) {
  return String(template || "").replace(/\{([^}]+)\}/g, (_, key) => {
    const k = key.trim();
    return context[k] !== undefined && context[k] !== null ? String(context[k]) : `{${k}}`;
  });
}

async function sendEmail(env, { to, subject, html }) {
  if (!env.RESEND_API_KEY || !env.MAIL_FROM) {
    return { provider_id: "mail-disabled" };
  }

  const resp = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.RESEND_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      from: env.MAIL_FROM,
      to: [to],
      subject,
      html,
    }),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Erreur d’envoi email: ${text}`);
  }

  const data = await resp.json();
  return { provider_id: data.id || "resend" };
}

async function supabaseSelect(env, table, select, query = {}) {
  const url = new URL(`${env.SUPABASE_URL}/rest/v1/${table}`);
  url.searchParams.set("select", select);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
  }

  const resp = await fetch(url.toString(), { headers: supabaseHeaders(env) });
  if (!resp.ok) throw new Error(`Erreur Supabase SELECT ${table}: ${await resp.text()}`);
  return await resp.json();
}

async function supabaseInsert(env, table, payload) {
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/${table}`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(env),
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) throw new Error(`Erreur Supabase INSERT ${table}: ${await resp.text()}`);
  const rows = await resp.json();
  return rows[0];
}

async function supabasePatch(env, table, id, payload) {
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/${table}?id=eq.${id}`, {
    method: "PATCH",
    headers: {
      ...supabaseHeaders(env),
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) throw new Error(`Erreur Supabase PATCH ${table}: ${await resp.text()}`);
  const rows = await resp.json();
  return rows[0];
}

function supabaseHeaders(env) {
  return {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
  };
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

function htmlResponse(html, status = 200) {
  return new Response(html, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

function withCors(response) {
  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  return new Response(response.body, { status: response.status, headers });
}

function renderAppHtml() {
  return `<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Pilotage manuel des scénarios</title>
  <style>
    body{font-family:Arial,sans-serif;background:#f5f7fb;color:#111827;margin:0}
    header{background:#111827;color:#fff;padding:18px 22px}
    main{padding:20px;max-width:1200px;margin:0 auto}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
    .card{background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:16px}
    h1,h2,h3{margin-top:0}.muted{color:#6b7280;font-size:13px}
    select,button,input{width:100%;padding:10px 12px;border:1px solid #d1d5db;border-radius:10px;margin-bottom:12px}
    button{background:#111827;color:#fff;cursor:pointer}
    button.secondary{background:#e5e7eb;color:#111827}
    .result{white-space:pre-wrap;background:#0f172a;color:#e2e8f0;border-radius:12px;padding:12px;font-size:13px;min-height:140px}
    table{width:100%;border-collapse:collapse;font-size:13px}.table-wrap{overflow:auto}
    th,td{padding:8px;border-bottom:1px solid #e5e7eb;text-align:left;vertical-align:top}
    @media (max-width: 900px){.grid{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <header>
    <h1>Pilotage manuel des scénarios</h1>
    <div class="muted" style="color:#cbd5e1">Lancement manuel, simulation, programmation différée et exécution automatique des envois.</div>
  </header>
  <main>
    <div class="grid">
      <section class="card">
        <h2>1. Choisir un événement</h2>
        <select id="eventSelect"></select>
        <div id="eventInfo" class="muted"></div>
      </section>
      <section class="card">
        <h2>2. Choisir un scénario</h2>
        <select id="scenarioSelect"></select>
        <div id="scenarioInfo" class="muted"></div>
      </section>
    </div>

    <section class="card" style="margin-top:16px;">
      <h2>3. Déclencher</h2>
      <button class="secondary" onclick="launch(true,false)">Simuler sans écrire en base</button>
      <button onclick="launch(false,false)">Programmer les messages</button>
      <button onclick="launch(false,true)">Programmer et envoyer tout ce qui est dû maintenant</button>
      <div class="muted">Pour programmer un second message 24h plus tard, ajoutez dans le step 2 : <code>logic_json.delay_hours_after_previous = 24</code>.</div>
    </section>

    <section class="card" style="margin-top:16px;">
      <h2>Résultat</h2>
      <div id="result" class="result">Aucune action exécutée.</div>
    </section>

    <section class="card" style="margin-top:16px;">
      <h2>Messages programmés</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>ID</th><th>Client</th><th>Scénario</th><th>Étape</th><th>Envoi prévu</th><th>Statut</th></tr></thead>
          <tbody id="jobsBody"></tbody>
        </table>
      </div>
      <button class="secondary" onclick="reloadJobs()">Rafraîchir les messages programmés</button>
      <button class="secondary" onclick="processDue()">Traiter les envois dus maintenant</button>
    </section>
  </main>

  <script>
    let events = [];
    let scenarios = [];

    async function boot() {
      events = await fetch('/api/events/manual').then(r => r.json());
      scenarios = await fetch('/api/scenarios').then(r => r.json());
      fillEvents();
      fillScenarios();
      await reloadJobs();
    }

    function fillEvents() {
      const el = document.getElementById('eventSelect');
      el.innerHTML = '';
      events.forEach(ev => {
        const opt = document.createElement('option');
        opt.value = ev.id;
        opt.textContent = `${ev.trigger_type?.label || ev.trigger_type?.code || 'Événement'} — zone ${ev.zone_cible} — ${ev.dedupe_key}`;
        el.appendChild(opt);
      });
      updateEventInfo();
      el.addEventListener('change', updateEventInfo);
    }

    function fillScenarios() {
      const el = document.getElementById('scenarioSelect');
      el.innerHTML = '';
      scenarios.forEach(sc => {
        const opt = document.createElement('option');
        opt.value = sc.id;
        opt.textContent = `${sc.label} (${sc.code})`;
        el.appendChild(opt);
      });
      updateScenarioInfo();
      el.addEventListener('change', updateScenarioInfo);
    }

    function updateEventInfo() {
      const id = Number(document.getElementById('eventSelect').value);
      const ev = events.find(x => x.id === id);
      document.getElementById('eventInfo').textContent = ev ? JSON.stringify(ev.payload || {}, null, 2) : '';
    }

    async function updateScenarioInfo() {
      const id = Number(document.getElementById('scenarioSelect').value);
      const sc = scenarios.find(x => x.id === id);
      if (!sc) return;
      const steps = await fetch(`/api/scenarios/${id}/steps`).then(r => r.json());
      const lines = steps.map(s => {
        const delay = Number(s.logic_json?.delay_hours_after_previous ?? 0);
        return `${s.code} — ordre ${s.step_order} — délai après précédent : ${delay}h`;
      });
      document.getElementById('scenarioInfo').textContent = `${sc.aggregation_mode} — priorité ${sc.priority}\n${lines.join('\n')}`;
    }

    async function launch(dry_run, trigger_send_immediately) {
      const event_id = Number(document.getElementById('eventSelect').value);
      const scenario_id = Number(document.getElementById('scenarioSelect').value);
      const res = await fetch('/api/manual-launch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ event_id, scenario_id, dry_run, trigger_send_immediately })
      });
      const data = await res.json();
      document.getElementById('result').textContent = JSON.stringify(data, null, 2);
      await reloadJobs();
    }

    async function reloadJobs() {
      const rows = await fetch('/api/jobs').then(r => r.json());
      const body = document.getElementById('jobsBody');
      body.innerHTML = '';
      rows.forEach(r => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${r.id}</td><td>${r.client_id}</td><td>${r.scenario_id}</td><td>${r.scenario_step_id}</td><td>${r.planned_send_at || ''}</td><td>${r.status}</td>`;
        body.appendChild(tr);
      });
    }

    async function processDue() {
      const data = await fetch('/api/process-due', { method: 'POST' }).then(r => r.json());
      document.getElementById('result').textContent = JSON.stringify(data, null, 2);
      await reloadJobs();
    }

    boot();
  </script>
</body>
</html>`;
}
