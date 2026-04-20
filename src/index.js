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
        return withCors(json({ ok: true, mode: "manual-scenario-option1" }));
      }

      if (url.pathname === "/api/debug/env" && request.method === "GET") {
        return withCors(json({
          has_SUPABASE_URL: !!env.SUPABASE_URL,
          has_SUPABASE_SERVICE_ROLE_KEY: !!env.SUPABASE_SERVICE_ROLE_KEY,
          has_RESEND_API_KEY: !!env.RESEND_API_KEY,
          has_MAIL_FROM: !!env.MAIL_FROM
        }));
      }

      if (url.pathname === "/api/scenarios" && request.method === "GET") {
        const scenarios = await supabaseSelect(
          env,
          "scenarios",
          "id,code,label,trigger_type_id,aggregation_mode,priority,active",
          { active: "eq.true", order: "priority.desc" }
        );
        return withCors(json(scenarios));
      }

      if (url.pathname.startsWith("/api/scenarios/") && url.pathname.endsWith("/steps") && request.method === "GET") {
        const scenarioId = Number(url.pathname.split("/")[3]);
        const steps = await supabaseSelect(
          env,
          "scenario_steps",
          "id,scenario_id,code,step_order,window_ref,window_min_hours,window_max_hours,logic_json,active",
          {
            scenario_id: "eq." + scenarioId,
            active: "eq.true",
            order: "step_order.asc"
          }
        );
        return withCors(json(steps));
      }

      if (url.pathname === "/api/clients/summary" && request.method === "GET") {
        const clients = await supabaseSelect(
          env,
          "clients",
          "id,email,zone_geo,active,siret",
          { active: "eq.true", order: "id.asc", limit: 1000 }
        );
        const zones = {};
        for (const c of clients) {
          const z = c.zone_geo || "(vide)";
          zones[z] = (zones[z] || 0) + 1;
        }
        return withCors(json({ total_clients: clients.length, zones }));
      }

      if (url.pathname === "/api/jobs" && request.method === "GET") {
        const jobs = await supabaseSelect(
          env,
          "client_message_items",
          "id,client_id,event_id,scenario_id,scenario_step_id,planned_send_at,priority,subject_rendered,status,created_at,sent_at",
          { order: "planned_send_at.asc", limit: 500 }
        );

        const clients = await supabaseSelect(
          env,
          "clients",
          "id,email,zone_geo,siret",
          { active: "eq.true", limit: 2000 }
        );
        const scenarios = await supabaseSelect(
          env,
          "scenarios",
          "id,code,label",
          { limit: 1000 }
        );
        const steps = await supabaseSelect(
          env,
          "scenario_steps",
          "id,scenario_id,code,step_order",
          { limit: 2000 }
        );

        const clientMap = new Map(clients.map(function (x) { return [x.id, x]; }));
        const scenarioMap = new Map(scenarios.map(function (x) { return [x.id, x]; }));
        const stepMap = new Map(steps.map(function (x) { return [x.id, x]; }));

        const enriched = jobs.map(function (j) {
          const client = clientMap.get(j.client_id) || null;
          const scenario = scenarioMap.get(j.scenario_id) || null;
          const step = stepMap.get(j.scenario_step_id) || null;

          return {
            ...j,
            client_email: client ? client.email : null,
            client_zone_geo: client ? client.zone_geo : null,
            scenario_code: scenario ? scenario.code : null,
            scenario_label: scenario ? scenario.label : null,
            step_code: step ? step.code : null,
            step_order: step ? step.step_order : null
          };
        });

        return withCors(json(enriched));
      }

      if (url.pathname === "/api/outbound-emails" && request.method === "GET") {
        const rows = await supabaseSelect(
          env,
          "outbound_emails",
          "id,client_id,send_date,planned_send_at,subject_rendered,status,created_at,sent_at",
          { order: "id.desc", limit: 300 }
        );

        const clients = await supabaseSelect(
          env,
          "clients",
          "id,email,zone_geo,siret",
          { active: "eq.true", limit: 2000 }
        );
        const clientMap = new Map(clients.map(function (x) { return [x.id, x]; }));

        const enriched = rows.map(function (r) {
          const client = clientMap.get(r.client_id) || null;
          return {
            ...r,
            client_email: client ? client.email : null,
            client_zone_geo: client ? client.zone_geo : null
          };
        });

        return withCors(json(enriched));
      }

      if (url.pathname === "/api/manual-launch" && request.method === "POST") {
        const body = await request.json();
        const result = await launchManualScenario(env, body);
        return withCors(json(result));
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
  }
};

async function launchManualScenario(env, payload) {
  const scenario_id = Number(payload && payload.scenario_id);
  const dry_run = !!(payload && payload.dry_run);
  const trigger_send_immediately = !!(payload && payload.trigger_send_immediately);
  const target_mode = (payload && payload.target_mode) || "all";
  const target_zone = payload && payload.target_zone ? String(payload.target_zone).trim() : "";
  const target_client_ids = Array.isArray(payload && payload.target_client_ids)
    ? payload.target_client_ids.map(Number).filter(Boolean)
    : [];
  const start_at = payload && payload.start_at ? new Date(payload.start_at) : new Date();

  if (!scenario_id) {
    throw new Error("scenario_id est obligatoire");
  }
  if (target_mode === "zone" && !target_zone) {
    throw new Error("target_zone est obligatoire quand target_mode = zone");
  }
  if (target_mode === "client_ids" && !target_client_ids.length) {
    throw new Error("target_client_ids est obligatoire quand target_mode = client_ids");
  }
  if (isNaN(start_at.getTime())) {
    throw new Error("start_at invalide");
  }

  const scenarioRows = await supabaseSelect(env, "scenarios", "*", {
    id: "eq." + scenario_id,
    active: "eq.true",
    limit: 1
  });
  const scenario = scenarioRows[0];
  if (!scenario) {
    throw new Error("Scénario introuvable ou inactif");
  }

  const steps = await supabaseSelect(env, "scenario_steps", "*", {
    scenario_id: "eq." + scenario.id,
    active: "eq.true",
    order: "step_order.asc"
  });
  if (!steps.length) {
    throw new Error("Aucune étape active sur ce scénario");
  }

  const clients = await loadManualTargetClients(env, target_mode, target_zone, target_client_ids);
  if (!clients.length) {
    throw new Error("Aucun client correspondant à la cible choisie");
  }

  const manualTriggerType = await getOrCreateManualTriggerType(env);

  const technicalEventPayload = {
    manual_launch: true,
    source: "ui",
    target_mode: target_mode,
    target_zone: target_zone || null,
    target_client_ids: target_client_ids,
    launched_at: new Date().toISOString()
  };

  const technicalEventPreview = {
    trigger_type_id: manualTriggerType.id,
    connecteur_id: null,
    source_external_id: null,
    dedupe_key: buildManualDedupeKey(scenario.id),
    zone_cible: target_mode === "zone" ? target_zone : "MANUAL",
    occurs_at: start_at.toISOString(),
    predicted_start_at: start_at.toISOString(),
    predicted_end_at: null,
    date_evenement: start_at.toISOString().slice(0, 10),
    severity: "info",
    payload: technicalEventPayload,
    statut: "open",
    validated_by: "manual-ui"
  };

  const contentItems = await supabaseSelect(env, "content_items", "*", {
    active: "eq.true"
  });

  const contentVersions = await supabaseSelect(env, "content_versions", "*", {
    status: "eq.published",
    order: "version_no.desc"
  });

  const itemByCode = new Map(contentItems.map(function (x) {
    return [x.code, x];
  }));

  const latestVersionByContentId = new Map();
  for (const v of contentVersions) {
    if (!latestVersionByContentId.has(v.content_item_id)) {
      latestVersionByContentId.set(v.content_item_id, v);
    }
  }

  const preview = [];
  let created = 0;
  let technicalEvent = null;

  if (!dry_run) {
    technicalEvent = await supabaseInsert(env, "events", technicalEventPreview);
  }

  for (const client of clients) {
    for (let index = 0; index < steps.length; index++) {
      const step = steps[index];
      const logic = step.logic_json || {};
      const rules = Array.isArray(logic.contents) ? logic.contents : [];
      const fakeEvent = technicalEvent || technicalEventPreview;

      const plannedAt = computeStepPlannedAt(fakeEvent, step);
      const renderContext = buildRenderContext(fakeEvent, client, step, scenario);

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

        if (!firstSubject && subject) {
          firstSubject = subject;
        }
        if (body) {
          renderedBlocks.push(body);
        }

        appliedVersions.push({
          content_code: rule.content_code,
          content_version_id: version.id
        });
      }

      if (!renderedBlocks.length) {
        continue;
      }

      const subjectRendered = firstSubject || ("[Prévention routière] " + scenario.label + " - " + step.code);
      const bodyRendered = renderedBlocks.join("\n\n");

      preview.push({
        client_id: client.id,
        client_email: client.email,
        scenario_id: scenario.id,
        scenario_label: scenario.label,
        scenario_code: scenario.code,
        scenario_step_id: step.id,
        step_code: step.code,
        step_window_ref: step.window_ref,
        step_window_min_hours: step.window_min_hours,
        step_window_max_hours: step.window_max_hours,
        planned_send_at: plannedAt.toISOString(),
        subject_rendered: subjectRendered
      });

      if (!dry_run) {
        await supabaseInsert(env, "client_message_items", {
          client_id: client.id,
          event_id: technicalEvent.id,
          scenario_id: scenario.id,
          scenario_step_id: step.id,
          planned_send_at: plannedAt.toISOString(),
          priority: scenario.priority || 50,
          subject_rendered: subjectRendered,
          body_rendered: bodyRendered,
          render_context: renderContext,
          applied_content_versions: appliedVersions,
          cooldown_key: "manual:" + technicalEvent.id + ":" + scenario.id + ":" + step.id + ":" + client.id,
          status: "ready",
          sent_at: null
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
    scenario_id: scenario.id,
    scenario_label: scenario.label,
    target_mode: target_mode,
    target_zone: target_zone || null,
    target_client_ids: target_client_ids,
    start_at: start_at.toISOString(),
    clients_concernes: clients.length,
    messages_programmes: dry_run ? preview.length : created,
    send_immediately: trigger_send_immediately,
    technical_event_id: technicalEvent ? technicalEvent.id : null,
    preview: preview
  };
}

function resolveReferenceDate(event, step) {
  const ref = step.window_ref;

  if (ref === "occurs_at" && event.occurs_at) {
    return new Date(event.occurs_at);
  }

  if (ref === "predicted_start_at" && event.predicted_start_at) {
    return new Date(event.predicted_start_at);
  }

  if (ref === "predicted_end_at" && event.predicted_end_at) {
    return new Date(event.predicted_end_at);
  }

  if (ref === "date_evenement" && event.date_evenement) {
    return new Date(event.date_evenement + "T08:00:00");
  }

  throw new Error("Impossible de calculer la référence temporelle pour l’étape " + step.code);
}

function computeStepPlannedAt(event, step) {
  const referenceDate = resolveReferenceDate(event, step);
  const maxHours = Number(step.window_max_hours || 0);
  return new Date(referenceDate.getTime() - maxHours * 3600 * 1000);
}

async function loadManualTargetClients(env, target_mode, target_zone, target_client_ids) {
  if (target_mode === "all") {
    return await supabaseSelect(env, "clients", "id,email,zone_geo,preferences,active,siret", {
      active: "eq.true",
      order: "id.asc",
      limit: 1000
    });
  }

  if (target_mode === "zone") {
    return await supabaseSelect(env, "clients", "id,email,zone_geo,preferences,active,siret", {
      active: "eq.true",
      zone_geo: "eq." + target_zone,
      order: "id.asc",
      limit: 1000
    });
  }

  if (target_mode === "client_ids") {
    const all = await supabaseSelect(env, "clients", "id,email,zone_geo,preferences,active,siret", {
      active: "eq.true",
      order: "id.asc",
      limit: 1000
    });
    const wanted = new Set(target_client_ids);
    return all.filter(function (c) {
      return wanted.has(c.id);
    });
  }

  throw new Error("target_mode invalide");
}

async function getOrCreateManualTriggerType(env) {
  const existing = await supabaseSelect(env, "trigger_types", "*", {
    code: "eq.MANUAL_TRIGGER",
    limit: 1
  });

  if (existing.length) {
    return existing[0];
  }

  return await supabaseInsert(env, "trigger_types", {
    code: "MANUAL_TRIGGER",
    label: "Déclenchement manuel",
    source_kind: "manual_ui",
    default_priority: 50,
    is_active: true
  });
}

function buildManualDedupeKey(scenarioId) {
  return "MANUAL_TRIGGER|scenario:" + scenarioId + "|" + new Date().toISOString();
}

async function processDueMessages(env) {
  const nowIso = new Date().toISOString();

  const dueItems = await supabaseSelect(
    env,
    "client_message_items",
    "id,client_id,event_id,scenario_id,scenario_step_id,planned_send_at,priority,subject_rendered,body_rendered,render_context,applied_content_versions,status",
    {
      status: "eq.ready",
      planned_send_at: "lte." + nowIso,
      order: "planned_send_at.asc",
      limit: 200
    }
  );

  if (!dueItems.length) {
    return { ok: true, processed: 0, sent: 0 };
  }

  const clients = await supabaseSelect(env, "clients", "id,email,zone_geo,siret", {
    active: "eq.true"
  });

  const clientMap = new Map(clients.map(function (c) {
    return [c.id, c];
  }));

  let sent = 0;

  for (const item of dueItems) {
    const client = clientMap.get(item.client_id);
    if (!client || !client.email) continue;

    const outbound = await supabaseInsert(env, "outbound_emails", {
      client_id: item.client_id,
      send_date: item.planned_send_at.slice(0, 10),
      planned_send_at: item.planned_send_at,
      subject_rendered: item.subject_rendered,
      body_rendered: item.body_rendered,
      status: "queued",
      presta_id: null,
      sent_at: null
    });

    await supabaseInsert(env, "outbound_email_items", {
      outbound_email_id: outbound.id,
      client_message_item_id: item.id,
      display_order: 1
    });

    const sendResult = await sendEmail(env, {
      to: client.email,
      subject: item.subject_rendered,
      html: item.body_rendered.replace(/\n/g, "<br>")
    });

    await supabasePatch(env, "outbound_emails", outbound.id, {
      status: "sent",
      presta_id: sendResult.provider_id || "mail-provider",
      sent_at: new Date().toISOString()
    });

    await supabasePatch(env, "client_message_items", item.id, {
      status: "sent",
      sent_at: new Date().toISOString()
    });

    await supabaseInsert(env, "envois_log", {
      outbound_email_id: outbound.id,
      client_id: item.client_id,
      event_id: item.event_id,
      sent_at: new Date().toISOString(),
      presta_id: sendResult.provider_id || "mail-provider",
      message: item.body_rendered
    });

    sent++;
  }

  return { ok: true, processed: dueItems.length, sent: sent };
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
    step_code: step.code
  };
}

function renderTemplate(template, context) {
  return String(template || "").replace(/\{([^}]+)\}/g, function (_, key) {
    const k = key.trim();
    return context[k] !== undefined && context[k] !== null ? String(context[k]) : "{" + k + "}";
  });
}

async function sendEmail(env, payload) {
  const to = payload.to;
  const subject = payload.subject;
  const html = payload.html;

  if (!env.RESEND_API_KEY || !env.MAIL_FROM) {
    return { provider_id: "mail-disabled" };
  }

  const resp = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      Authorization: "Bearer " + env.RESEND_API_KEY,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      from: env.MAIL_FROM,
      to: [to],
      subject: subject,
      html: html
    })
  });

  if (!resp.ok) {
    throw new Error("Erreur d’envoi email: " + (await resp.text()));
  }

  const data = await resp.json();
  return { provider_id: data.id || "resend" };
}

async function supabaseSelect(env, table, select, query) {
  ensureSupabaseEnv(env);
  const baseUrl = String(env.SUPABASE_URL).trim().replace(/\/+$/, "");
  const url = new URL(baseUrl + "/rest/v1/" + table);
  url.searchParams.set("select", select);

  const safeQuery = query || {};
  for (const entry of Object.entries(safeQuery)) {
    const k = entry[0];
    const v = entry[1];
    if (v !== undefined && v !== null && v !== "") {
      url.searchParams.set(k, v);
    }
  }

  const resp = await fetch(url.toString(), { headers: supabaseHeaders(env) });
  if (!resp.ok) {
    throw new Error("Erreur Supabase SELECT " + table + ": " + (await resp.text()));
  }
  return await resp.json();
}

async function supabaseInsert(env, table, payload) {
  ensureSupabaseEnv(env);
  const baseUrl = String(env.SUPABASE_URL).trim().replace(/\/+$/, "");

  const resp = await fetch(baseUrl + "/rest/v1/" + table, {
    method: "POST",
    headers: {
      ...supabaseHeaders(env),
      "Content-Type": "application/json",
      Prefer: "return=representation"
    },
    body: JSON.stringify(payload)
  });

  if (!resp.ok) {
    throw new Error("Erreur Supabase INSERT " + table + ": " + (await resp.text()));
  }
  const rows = await resp.json();
  return rows[0];
}

async function supabasePatch(env, table, id, payload) {
  ensureSupabaseEnv(env);
  const baseUrl = String(env.SUPABASE_URL).trim().replace(/\/+$/, "");

  const resp = await fetch(baseUrl + "/rest/v1/" + table + "?id=eq." + id, {
    method: "PATCH",
    headers: {
      ...supabaseHeaders(env),
      "Content-Type": "application/json",
      Prefer: "return=representation"
    },
    body: JSON.stringify(payload)
  });

  if (!resp.ok) {
    throw new Error("Erreur Supabase PATCH " + table + ": " + (await resp.text()));
  }
  const rows = await resp.json();
  return rows[0];
}

function ensureSupabaseEnv(env) {
  if (!env.SUPABASE_URL) {
    throw new Error("SUPABASE_URL manquante dans les variables d’environnement Cloudflare");
  }
  if (!env.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("SUPABASE_SERVICE_ROLE_KEY manquante dans les variables d’environnement Cloudflare");
  }
}

function supabaseHeaders(env) {
  return {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: "Bearer " + env.SUPABASE_SERVICE_ROLE_KEY
  };
}

function json(data, status) {
  return new Response(JSON.stringify(data, null, 2), {
    status: status || 200,
    headers: { "Content-Type": "application/json; charset=utf-8" }
  });
}

function htmlResponse(html, status) {
  return new Response(html, {
    status: status || 200,
    headers: { "Content-Type": "text/html; charset=utf-8" }
  });
}

function withCors(response) {
  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  return new Response(response.body, { status: response.status, headers: headers });
}

function renderAppHtml() {
  return `<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Pilotage des envois</title>
  <style>
    body { font-family: Arial, sans-serif; background:#f5f7fb; color:#111827; margin:0; }
    header { background:#111827; color:#fff; padding:18px 22px; }
    main { padding:20px; max-width:1280px; margin:0 auto; }
    .tabs { display:flex; gap:10px; margin-bottom:16px; flex-wrap:wrap; }
    .tab { background:#e5e7eb; padding:10px 14px; border-radius:10px; cursor:pointer; border:none; }
    .tab.active { background:#111827; color:#fff; }
    .panel { display:none; }
    .panel.active { display:block; }
    .grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .card { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:16px; margin-bottom:16px; }
    .muted { color:#6b7280; font-size:13px; }
    .help { color:#6b7280; font-size:12px; margin-top:-6px; margin-bottom:10px; }
    h1,h2,h3 { margin-top:0; }
    button,input,select,textarea {
      padding:10px 12px;
      border-radius:10px;
      border:1px solid #d1d5db;
      box-sizing:border-box;
      font-size:14px;
      width:100%;
    }
    button { cursor:pointer; background:#111827; color:#fff; border:none; }
    button.secondary { background:#e5e7eb; color:#111827; }
    button.success { background:#065f46; }
    .row { display:flex; gap:10px; flex-wrap:wrap; }
    .row > * { flex:1; }
    .result {
      white-space:pre-wrap;
      background:#0f172a;
      color:#e2e8f0;
      border-radius:12px;
      padding:12px;
      font-size:13px;
      min-height:140px;
    }
    .table-wrap { overflow:auto; }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th,td { padding:8px; border-bottom:1px solid #e5e7eb; text-align:left; vertical-align:top; }
    th { background:#f9fafb; position:sticky; top:0; }
    .pill { display:inline-block; background:#f3f4f6; border-radius:999px; padding:4px 8px; font-size:12px; }
    .preview-box {
      white-space:pre-wrap;
      background:#fff;
      border:1px solid #e5e7eb;
      border-radius:12px;
      padding:12px;
      min-height:220px;
    }
    .kpis { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; }
    .kpi { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:16px; }
    .kpi .v { font-size:28px; font-weight:bold; }
    code { background:#f3f4f6; padding:2px 6px; border-radius:6px; color:#111827; }
    @media (max-width: 980px) {
      .grid, .kpis { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Pilotage des envois</h1>
    <div class="muted" style="color:#cbd5e1;">Suivi automatique + lancement manuel d’un scénario</div>
  </header>

  <main>
    <div class="tabs">
      <button type="button" onclick="showPanel('auto')" id="tabAuto" class="tab active">Suivi automatique</button>
      <button type="button" onclick="showPanel('manual')" id="tabManual" class="tab">Lancement manuel</button>
    </div>

    <div id="panelAuto" class="panel active">
      <div class="kpis">
        <div class="kpi"><div class="muted">Messages programmés</div><div class="v" id="kpiJobs">0</div></div>
        <div class="kpi"><div class="muted">Prêts à partir</div><div class="v" id="kpiReady">0</div></div>
        <div class="kpi"><div class="muted">Déjà envoyés</div><div class="v" id="kpiSent">0</div></div>
        <div class="kpi"><div class="muted">Emails générés</div><div class="v" id="kpiEmails">0</div></div>
      </div>

      <div class="card" style="margin-top:16px;">
        <h2>Messages programmés</h2>
        <div class="row" style="margin-bottom:12px;">
          <input id="jobsFilter" placeholder="Filtrer par client, scénario, statut..." />
          <button type="button" class="secondary" onclick="reloadJobsAndEmails()">Rafraîchir</button>
          <button type="button" class="secondary" onclick="processDue()">Traiter les envois dus maintenant</button>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Client</th>
                <th>Scénario</th>
                <th>Étape</th>
                <th>Prévu le</th>
                <th>Objet</th>
                <th>Statut</th>
              </tr>
            </thead>
            <tbody id="jobsBody"></tbody>
          </table>
        </div>
      </div>

      <div class="card">
        <h2>Emails générés</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Client</th>
                <th>Date d’envoi</th>
                <th>Objet</th>
                <th>Statut</th>
              </tr>
            </thead>
            <tbody id="emailsBody"></tbody>
          </table>
        </div>
      </div>
    </div>

    <div id="panelManual" class="panel">
      <div class="grid">
        <div class="card">
          <h2>1. Choisir un scénario</h2>
          <select id="scenarioSelect"></select>
          <div class="help">Choisissez le scénario que vous souhaitez lancer manuellement.</div>
          <div id="scenarioInfo" class="preview-box"></div>
        </div>

        <div class="card">
          <h2>2. Choisir la cible</h2>
          <select id="targetMode">
            <option value="all">Tous les clients actifs</option>
            <option value="zone">Clients d’une zone</option>
          </select>
          <input id="targetZone" placeholder="Ex. 56" style="display:none;" />
          <div class="help">En mode manuel, l’interface crée en interne un événement technique caché de type MANUAL_TRIGGER.</div>
          <div id="clientSummary" class="preview-box"></div>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <h2>3. Date et heure de référence</h2>
          <input id="startAt" type="datetime-local" />
          <div class="help">Cette date sert de référence pour calculer la programmation de chaque étape, selon sa référence temporelle et sa fenêtre.</div>
        </div>

        <div class="card">
          <h2>4. Aperçu du lancement</h2>
          <div class="row">
            <button type="button" class="secondary" onclick="simulateManual()">Simuler</button>
            <button type="button" onclick="launchManual(false)">Programmer</button>
            <button type="button" class="success" onclick="launchManual(true)">Programmer et envoyer ce qui est dû maintenant</button>
          </div>
          <div class="help">La simulation vous montre la date réelle programmée pour chaque étape, en respectant la logique temporelle définie dans l’admin.</div>
        </div>
      </div>

      <div class="card">
        <h2>Résultat / simulation</h2>
        <div id="result" class="result">Aucune action exécutée.</div>
      </div>

      <div class="card">
        <h2>Prévisualisation humaine</h2>
        <div class="help">Chaque étape est programmée à l’entrée de sa fenêtre temporelle, telle qu’elle a été définie dans l’interface d’administration.</div>
        <div id="manualPreview" class="preview-box">Lancez une simulation pour voir le détail.</div>
      </div>
    </div>
  </main>

  <script>
    var scenarios = [];
    var jobsData = [];
    var emailsData = [];

    function showPanel(name) {
      document.getElementById('panelAuto').classList.remove('active');
      document.getElementById('panelManual').classList.remove('active');
      document.getElementById('tabAuto').classList.remove('active');
      document.getElementById('tabManual').classList.remove('active');

      if (name === 'auto') {
        document.getElementById('panelAuto').classList.add('active');
        document.getElementById('tabAuto').classList.add('active');
      } else {
        document.getElementById('panelManual').classList.add('active');
        document.getElementById('tabManual').classList.add('active');
      }
    }

    async function boot() {
      try {
        var scenariosResp = await fetch('/api/scenarios');
        var scenariosData = await scenariosResp.json();

        if (!scenariosResp.ok) {
          throw new Error('Erreur /api/scenarios : ' + (scenariosData.error || 'erreur inconnue'));
        }

        if (!Array.isArray(scenariosData)) {
          throw new Error('/api/scenarios ne renvoie pas un tableau');
        }

        scenarios = scenariosData;
        fillScenarios();
        setDefaultStartAt();
        toggleTargetInputs();
        await loadClientSummary();
        await reloadJobsAndEmails();

        document.getElementById('jobsFilter').addEventListener('input', renderJobsTable);
        document.getElementById('targetMode').addEventListener('change', toggleTargetInputs);
        document.getElementById('targetZone').addEventListener('input', loadClientSummary);
      } catch (e) {
        document.getElementById('result').textContent = 'Erreur au chargement :\\n\\n' + (e.message || String(e));
      }
    }

    function setDefaultStartAt() {
      var d = new Date();
      d.setMinutes(d.getMinutes() - d.getTimezoneOffset());
      document.getElementById('startAt').value = d.toISOString().slice(0, 16);
    }

    function fillScenarios() {
      var el = document.getElementById('scenarioSelect');
      el.innerHTML = '';

      scenarios.forEach(function(sc) {
        var opt = document.createElement('option');
        opt.value = sc.id;
        opt.textContent = sc.label + ' (' + sc.code + ')';
        el.appendChild(opt);
      });

      updateScenarioInfo();
      el.addEventListener('change', updateScenarioInfo);
    }

    function translateWindowRef(ref) {
      if (ref === 'occurs_at') return 'Date de survenance';
      if (ref === 'predicted_start_at') return 'Début prévu';
      if (ref === 'predicted_end_at') return 'Fin prévue';
      if (ref === 'date_evenement') return 'Date d’événement';
      return ref || 'Non renseigné';
    }

    function formatDateFr(value) {
      if (!value) return '';
      var d = new Date(value);
      if (isNaN(d.getTime())) return value;
      return d.toLocaleString('fr-FR');
    }

    async function updateScenarioInfo() {
      var id = Number(document.getElementById('scenarioSelect').value);
      var sc = scenarios.find(function(x) { return x.id === id; });
      if (!sc) return;
    
      var steps = await fetch('/api/scenarios/' + id + '/steps').then(function(r) { return r.json(); });
    
      var lines = [];
      lines.push('Scénario sélectionné');
      lines.push('');
      lines.push('Nom : ' + sc.label);
      lines.push('Code : ' + sc.code);
      lines.push('Mode : ' + sc.aggregation_mode);
      lines.push('Priorité : ' + sc.priority);
      lines.push('');
    
      lines.push('Ordre des messages prévus :');
    
      if (Array.isArray(steps) && steps.length) {
        steps.forEach(function(s, idx) {
          lines.push(
            (idx + 1) + '. Étape ' + s.code +
            ' | référence : ' + translateWindowRef(s.window_ref) +
            ' | fenêtre : ' + s.window_min_hours + 'h → ' + s.window_max_hours + 'h'
          );
        });
      } else {
        lines.push('Aucune étape active');
      }
    
      document.getElementById('scenarioInfo').textContent = lines.join('\n');
    }

    function toggleTargetInputs() {
      var mode = document.getElementById('targetMode').value;
      document.getElementById('targetZone').style.display = mode === 'zone' ? 'block' : 'none';
      loadClientSummary();
    }

    async function loadClientSummary() {
        var data = await fetch('/api/clients/summary').then(function(r) { return r.json(); });
        var mode = document.getElementById('targetMode').value;
        var zone = document.getElementById('targetZone').value.trim();
        var lines = [];
      
        lines.push('Résumé de la cible');
        lines.push('');
      
        if (mode === 'all') {
          lines.push('Cible choisie : tous les clients actifs');
        } else if (mode === 'zone') {
          lines.push('Cible choisie : clients de la zone ' + (zone || '(non renseignée)'));
        }
      
        lines.push('Nombre total de clients actifs : ' + data.total_clients);
      
        var zones = data.zones || {};
        var zoneKeys = Object.keys(zones).sort();
      
        if (zoneKeys.length) {
          lines.push('');
          lines.push('Répartition par zone :');
          zoneKeys.forEach(function(z) {
            lines.push('- Zone ' + z + ' : ' + zones[z] + ' client(s)');
          });
        }
  
    document.getElementById('clientSummary').textContent = lines.join('\n');
  }

    function buildManualPayload(dryRun, sendNow) {
      var scenario_id = Number(document.getElementById('scenarioSelect').value);
      var target_mode = document.getElementById('targetMode').value;
      var target_zone = document.getElementById('targetZone').value.trim();
      var start_at = document.getElementById('startAt').value
        ? new Date(document.getElementById('startAt').value).toISOString()
        : null;

      return {
        scenario_id: scenario_id,
        target_mode: target_mode,
        target_zone: target_zone,
        start_at: start_at,
        dry_run: dryRun,
        trigger_send_immediately: sendNow
      };
    }

    async function simulateManual() {
      var payload = buildManualPayload(true, false);

      var res = await fetch('/api/manual-launch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      var data = await res.json();
      document.getElementById('result').textContent = JSON.stringify(data, null, 2);
      renderManualPreview(data);
    }

    async function launchManual(sendNow) {
      var payload = buildManualPayload(false, sendNow);

      var res = await fetch('/api/manual-launch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      var data = await res.json();
      document.getElementById('result').textContent = JSON.stringify(data, null, 2);
      renderManualPreview(data);
      await reloadJobsAndEmails();
    }

    function renderManualPreview(data) {
      var box = document.getElementById('manualPreview');
    
      if (!data || !Array.isArray(data.preview)) {
        box.textContent = 'Aucune prévisualisation disponible.';
        return;
      }
    
      if (!data.preview.length) {
        box.textContent = 'Aucun message ne serait programmé pour la cible choisie.';
        return;
      }
    
      var lines = [];
      lines.push('Résumé du lancement manuel');
      lines.push('');
      lines.push('Scénario : ' + (data.scenario_label || ''));
      lines.push('Nombre de clients concernés : ' + (data.clients_concernes || 0));
      lines.push('Nombre total de messages programmés : ' + (data.messages_programmes || 0));
      lines.push('');
    
      var grouped = {};
      data.preview.forEach(function(item) {
        var key = item.client_email || ('client-' + item.client_id);
        if (!grouped[key]) grouped[key] = [];
        grouped[key].push(item);
      });
    
      Object.keys(grouped).forEach(function(clientEmail) {
        var items = grouped[clientEmail].slice().sort(function(a, b) {
          var da = new Date(a.planned_send_at).getTime();
          var db = new Date(b.planned_send_at).getTime();
          return da - db;
        });
    
        lines.push('Destinataire : ' + clientEmail);
        lines.push('Ordre des messages :');
    
        items.forEach(function(item, index) {
          lines.push(
            '  ' + (index + 1) + '. ' +
            (item.step_code || ('Étape ' + item.scenario_step_id))
          );
          lines.push('     Objet : ' + (item.subject_rendered || 'Sans objet'));
          lines.push('     Envoi prévu : ' + formatDateFr(item.planned_send_at));
          lines.push(
            '     Base de calcul : ' +
            translateWindowRef(item.step_window_ref) +
            ' | fenêtre ' +
            item.step_window_min_hours + 'h → ' + item.step_window_max_hours + 'h'
          );
        });
    
        lines.push('');
      });
    
      box.textContent = lines.join('\n');
    }

    async function reloadJobsAndEmails() {
      await reloadJobs();
      await reloadEmails();
    }

    function computeKpis() {
      var total = jobsData.length;
      var ready = jobsData.filter(function(x) { return x.status === 'ready'; }).length;
      var sent = jobsData.filter(function(x) { return x.status === 'sent'; }).length;

      document.getElementById('kpiJobs').textContent = String(total);
      document.getElementById('kpiReady').textContent = String(ready);
      document.getElementById('kpiSent').textContent = String(sent);
      document.getElementById('kpiEmails').textContent = String(emailsData.length);
    }

    async function reloadJobs() {
      var rows = await fetch('/api/jobs').then(function(r) { return r.json(); });
      jobsData = Array.isArray(rows) ? rows : [];
      renderJobsTable();
      computeKpis();
    }

    function renderJobsTable() {
      var filter = (document.getElementById('jobsFilter').value || '').toLowerCase();
      var body = document.getElementById('jobsBody');
      body.innerHTML = '';

      jobsData
        .filter(function(r) {
          if (!filter) return true;
          var txt = [
            r.id,
            r.client_email,
            r.client_id,
            r.scenario_label,
            r.scenario_code,
            r.step_code,
            r.scenario_step_id,
            r.planned_send_at,
            r.subject_rendered,
            r.status
          ].join(' ').toLowerCase();
          return txt.indexOf(filter) !== -1;
        })
        .forEach(function(r) {
          var tr = document.createElement('tr');
          tr.innerHTML =
            '<td>' + r.id + '</td>' +
            '<td>' + (r.client_email || r.client_id) + '</td>' +
            '<td>' + (r.scenario_label || r.scenario_code || r.scenario_id) + '</td>' +
            '<td>' + (r.step_code || r.scenario_step_id) + '</td>' +
            '<td>' + (r.planned_send_at || '') + '</td>' +
            '<td>' + (r.subject_rendered || '') + '</td>' +
            '<td><span class="pill">' + r.status + '</span></td>';
          body.appendChild(tr);
        });
    }

    async function reloadEmails() {
      var rows = await fetch('/api/outbound-emails').then(function(r) { return r.json(); });
      emailsData = Array.isArray(rows) ? rows : [];
      var body = document.getElementById('emailsBody');
      body.innerHTML = '';

      emailsData.forEach(function(r) {
        var tr = document.createElement('tr');
        tr.innerHTML =
          '<td>' + r.id + '</td>' +
          '<td>' + (r.client_email || r.client_id) + '</td>' +
          '<td>' + (r.send_date || '') + '</td>' +
          '<td>' + (r.subject_rendered || '') + '</td>' +
          '<td><span class="pill">' + r.status + '</span></td>';
        body.appendChild(tr);
      });

      computeKpis();
    }

    async function processDue() {
      var data = await fetch('/api/process-due', { method: 'POST' }).then(function(r) { return r.json(); });
      document.getElementById('result').textContent = JSON.stringify(data, null, 2);
      await reloadJobsAndEmails();
    }

    boot();
  </script>
</body>
</html>`;
}
