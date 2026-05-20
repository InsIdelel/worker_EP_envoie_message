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
          has_MAILGUN_API_KEY: !!env.MAILGUN_API_KEY,
          has_MAILGUN_DOMAIN: !!env.MAILGUN_DOMAIN,
          has_MAILGUN_BASE_URL: !!env.MAILGUN_BASE_URL,
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

        return withCors(json({
          total_clients: clients.length,
          zones
        }));
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

function buildStepRenderedMessage(step, scenario, event, client, itemByCode, latestVersionByContentId) {
  const logic = step.logic_json || {};
  const rules = Array.isArray(logic.contents) ? logic.contents : [];
  const renderContext = buildRenderContext(event, client, step, scenario);

  const renderedBlocks = [];
  const appliedVersions = [];
  const previewBlocks = [];

  let firstSubject = "";
  let firstChannel = "";

  for (const rule of rules) {
    const contentItem = itemByCode.get(rule.content_code);
    if (!contentItem) continue;

    const version = latestVersionByContentId.get(contentItem.id);
    if (!version) continue;

    const subject = renderTemplate(version.sujet_template || contentItem.sujet || "", renderContext);
    const body = renderTemplate(version.corps_template || "", renderContext);
    const channel = String(contentItem.channel || "email").toLowerCase();

    if (!firstSubject && subject) firstSubject = subject;
    if (!firstChannel && channel) firstChannel = channel;

    if (body) {
      renderedBlocks.push(body);
    }

    appliedVersions.push({
      content_code: rule.content_code,
      content_item_id: contentItem.id,
      content_version_id: version.id,
      subject_rendered: subject,
      channel: channel
    });

    previewBlocks.push({
      content_code: rule.content_code,
      content_title: contentItem.sujet || rule.content_code,
      subject_rendered: subject,
      channel: channel,
      condition: rule.when || "always"
    });
  }

  if (!renderedBlocks.length) return null;

  const subjectRendered =
    firstSubject ||
    ("[Prévention routière] " + scenario.label + " - " + step.code);

  return {
    subjectRendered,
    bodyRendered: renderedBlocks.join("\n\n"),
    renderContext,
    appliedVersions,
    previewBlocks,
    contentCount: previewBlocks.length,
    isGroupedStep: previewBlocks.length > 1,
    channel: firstChannel || "email"
  };
}

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
    for (const step of steps) {
      const fakeEvent = technicalEvent || technicalEventPreview;
      const plannedAt = computeStepPlannedAt(fakeEvent, step);

      const stepMessage = buildStepRenderedMessage(
        step,
        scenario,
        fakeEvent,
        client,
        itemByCode,
        latestVersionByContentId
      );

      if (!stepMessage) {
        continue;
      }

      const subjectRendered = stepMessage.subjectRendered;
      const bodyRendered = stepMessage.bodyRendered;

      preview.push({
        client_id: client.id,
        client_email: client.email,
        scenario_id: scenario.id,
        scenario_label: scenario.label,
        scenario_code: scenario.code,
        scenario_aggregation_mode: scenario.aggregation_mode,
        scenario_step_id: step.id,
        step_code: step.code,
        step_order: step.step_order,
        step_window_ref: step.window_ref,
        step_window_min_hours: step.window_min_hours,
        step_window_max_hours: step.window_max_hours,
        planned_send_at: plannedAt.toISOString(),
        subject_rendered: subjectRendered,
        channel: stepMessage.channel,
        content_count: stepMessage.contentCount,
        is_grouped_step: stepMessage.isGroupedStep,
        contents: stepMessage.previewBlocks
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
          render_context: stepMessage.renderContext,
          applied_content_versions: stepMessage.appliedVersions,
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

async function getExistingOutboundEmail(env, clientId, sendDate) {
  const rows = await supabaseSelect(
    env,
    "outbound_emails",
    "id,client_id,send_date,planned_send_at,subject_rendered,body_rendered,status,presta_id,sent_at",
    {
      client_id: "eq." + clientId,
      send_date: "eq." + sendDate,
      limit: 1
    }
  );

  return rows[0] || null;
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
      limit: 500
    }
  );

  if (!dueItems.length) {
    return {
      ok: true,
      processed: 0,
      sent: 0,
      grouped_emails: 0,
      message: "Aucun envoi dû."
    };
  }

  const clients = await supabaseSelect(env, "clients", "id,email,zone_geo,siret", {
    active: "eq.true"
  });

  const clientMap = new Map(clients.map(function (c) {
    return [c.id, c];
  }));

  const groups = groupDueItemsByClientAndDate(dueItems);

  let sentGroups = 0;
  let sentItems = 0;
  const logs = [];

  for (const group of groups) {
    try {
      const client = clientMap.get(group.client_id);

      if (!client || !client.email) {
        logs.push({
          group_key: group.key,
          step: "client_lookup",
          status: "skipped",
          reason: "client introuvable ou sans email"
        });
        continue;
      }

      const sortedItems = group.items.slice().sort(function (a, b) {
        return new Date(a.planned_send_at).getTime() - new Date(b.planned_send_at).getTime();
      });

      const subject = buildAggregatedSubject(sortedItems);
      const html = buildAggregatedHtml(sortedItems);
      const text = stripHtml(html);

      let outbound = await getExistingOutboundEmail(env, group.client_id, group.send_date);

      if (outbound) {
        logs.push({
          group_key: group.key,
          step: "find_outbound_email",
          status: "ok",
          mode: "existing",
          outbound_email_id: outbound.id
        });

        await supabasePatch(env, "outbound_emails", outbound.id, {
          planned_send_at: sortedItems[0].planned_send_at,
          subject_rendered: subject,
          body_rendered: html,
          status: "queued",
          presta_id: null,
          sent_at: null
        });
      } else {
        outbound = await supabaseInsert(env, "outbound_emails", {
          client_id: group.client_id,
          send_date: group.send_date,
          planned_send_at: sortedItems[0].planned_send_at,
          subject_rendered: subject,
          body_rendered: html,
          status: "queued",
          presta_id: null,
          sent_at: null
        });

        logs.push({
          group_key: group.key,
          step: "insert_outbound_emails",
          status: "ok",
          mode: "created",
          outbound_email_id: outbound.id
        });
      }

      for (let i = 0; i < sortedItems.length; i++) {
        const item = sortedItems[i];

        const existingLink = await supabaseSelect(
          env,
          "outbound_email_items",
          "id,outbound_email_id,client_message_item_id",
          {
            outbound_email_id: "eq." + outbound.id,
            client_message_item_id: "eq." + item.id,
            limit: 1
          }
        );

        if (!existingLink.length) {
          await supabaseInsert(env, "outbound_email_items", {
            outbound_email_id: outbound.id,
            client_message_item_id: item.id,
            display_order: i + 1
          });
        }
      }

      logs.push({
        group_key: group.key,
        step: "insert_outbound_email_items",
        status: "ok",
        items_count: sortedItems.length
      });

      const sendResult = await sendEmail(env, {
        to: client.email,
        subject: subject,
        html: html,
        text: text
      });

      logs.push({
        group_key: group.key,
        step: "mailgun_send",
        status: "ok",
        provider_id: sendResult.provider_id || null,
        client_email: client.email
      });

      await supabasePatch(env, "outbound_emails", outbound.id, {
        status: "sent",
        presta_id: sendResult.provider_id || "mail-provider",
        sent_at: new Date().toISOString()
      });

      for (const item of sortedItems) {
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

        sentItems++;
      }

      logs.push({
        group_key: group.key,
        step: "finalize",
        status: "ok",
        outbound_email_id: outbound.id,
        items_sent: sortedItems.length
      });

      sentGroups++;
    } catch (e) {
      logs.push({
        group_key: group.key,
        status: "error",
        error: e.message || String(e)
      });

      return {
        ok: false,
        processed: dueItems.length,
        sent: sentItems,
        grouped_emails: sentGroups,
        failed_group: group.key,
        logs: logs,
        error: e.message || String(e)
      };
    }
  }

  return {
    ok: true,
    processed: dueItems.length,
    sent: sentItems,
    grouped_emails: sentGroups,
    logs: logs
  };
}

function groupDueItemsByClientAndDate(items) {
  const map = new Map();

  for (const item of items) {
    const sendDate = item.planned_send_at.slice(0, 10);
    const key = item.client_id + "|" + sendDate;

    if (!map.has(key)) {
      map.set(key, {
        key: key,
        client_id: item.client_id,
        send_date: sendDate,
        items: []
      });
    }

    map.get(key).items.push(item);
  }

  return Array.from(map.values());
}

function buildAggregatedSubject(items) {
  if (!items || !items.length) {
    return "Prévention routière";
  }

  if (items.length === 1) {
    return items[0].subject_rendered || "Prévention routière";
  }

  return "Prévention routière : " + items.length + " points de vigilance pour vos équipes";
}

function buildAggregatedHtml(items) {
  const intro =
    '<p>Bonjour,</p>' +
    '<p>Voici les points de vigilance identifiés pour vos équipes :</p>';

  const blocks = items.map(function (item, index) {
    return (
      '<hr style="border:none;border-top:1px solid #e5e7eb;margin:24px 0;" />' +
      '<h3 style="margin:0 0 12px 0;font-family:Arial,sans-serif;">' +
      (index + 1) + '. ' + escapeHtmlForEmail(item.subject_rendered || "Point de vigilance") +
      '</h3>' +
      '<div style="font-family:Arial,sans-serif;line-height:1.5;">' +
      (item.body_rendered || '') +
      '</div>'
    );
  });

  const outro =
    '<p style="margin-top:24px;">Bonne diffusion interne.</p>';

  return (
    '<div style="font-family:Arial,sans-serif;font-size:14px;color:#280c10;">' +
    intro +
    blocks.join('') +
    outro +
    '</div>'
  );
}

function escapeHtmlForEmail(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
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
  const html = payload.html || "";
  const text = payload.text || stripHtml(html);

  if (!env.MAILGUN_API_KEY) throw new Error("MAILGUN_API_KEY manquante");
  if (!env.MAILGUN_DOMAIN) throw new Error("MAILGUN_DOMAIN manquant");
  if (!env.MAIL_FROM) throw new Error("MAIL_FROM manquant");

  const baseUrl = (env.MAILGUN_BASE_URL || "https://api.mailgun.net").replace(/\/+$/, "");
  const url = baseUrl + "/v3/" + env.MAILGUN_DOMAIN + "/messages";

  const form = new FormData();
  form.append("from", env.MAIL_FROM);
  form.append("to", to);
  form.append("subject", subject);
  form.append("html", html);
  form.append("text", text);

  const auth = "Basic " + btoa("api:" + env.MAILGUN_API_KEY);

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: auth
    },
    body: form
  });

  if (!resp.ok) {
    throw new Error("Erreur d’envoi Mailgun: " + (await resp.text()));
  }

  const data = await resp.json();
  return { provider_id: data.id || "mailgun" };
}

function stripHtml(html) {
  return String(html || "")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/\n\s+\n/g, "\n\n")
    .replace(/[ \t]+/g, " ")
    .trim();
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

  const resp = await fetch(url.toString(), {
    headers: supabaseHeaders(env)
  });

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

  return new Response(response.body, {
    status: response.status,
    headers: headers
  });
}

function renderAppHtml() {
  return `<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MobiSûr – Pilotage des envois</title>
  <style>
    :root {
      --brand-brown: #280c10;
      --brand-blue: #3633d2;
      --brand-blue-2: #3431d1;
      --brand-yellow: #ffff66;
      --bg: #f7f7fb;
      --panel: rgba(255,255,255,.96);
      --line: #e6e3ef;
      --muted: #6f6570;
      --soft-blue: #f0f0ff;
      --soft-yellow: #ffffe2;
      --danger: #b91c1c;
      --success: #0f766e;
      --shadow: 0 18px 50px rgba(40, 12, 16, .10);
      --radius: 24px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(54, 51, 210, .13), transparent 34%),
        radial-gradient(circle at top right, rgba(255, 255, 102, .35), transparent 24%),
        var(--bg);
      color: var(--brand-brown);
    }

    header {
      padding: 26px 24px;
      background: white;
      border-bottom: 1px solid var(--line);
    }

    .brand {
      max-width: 1280px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      gap: 18px;
    }

    .brand-mark {
      width: 82px;
      height: 82px;
      flex: 0 0 82px;
      position: relative;
      border-radius: 50%;
      background: #fff;
      border: 8px solid var(--brand-brown);
      box-shadow: 0 12px 30px rgba(40,12,16,.14);
    }

    .brand-logo {
      width: min(420px, 72vw);
      height: auto;
      display: block;
    }
    
    .brand {
      max-width: 1280px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      gap: 22px;
      flex-wrap: wrap;
    }
    
    .brand-copy {
      min-width: 260px;
    }

    .brand-copy h1 {
      margin: 0;
      font-size: clamp(30px, 5vw, 52px);
      line-height: .95;
      letter-spacing: -.06em;
      color: var(--brand-brown);
    }

    .brand-copy .baseline {
      margin-top: 7px;
      font-size: clamp(18px, 2.8vw, 30px);
      font-weight: 850;
      letter-spacing: -.04em;
      color: var(--brand-blue);
    }

    main {
      padding: 22px;
      max-width: 1280px;
      margin: 0 auto;
    }

    .tabs {
      display: flex;
      gap: 10px;
      margin: 4px 0 18px;
      flex-wrap: wrap;
    }

    .tab {
      width: auto;
      border: 1px solid var(--line);
      background: white;
      color: var(--brand-brown);
      border-radius: 999px;
      padding: 12px 16px;
      font-weight: 900;
      cursor: pointer;
      box-shadow: 0 8px 20px rgba(40,12,16,.04);
    }

    .tab.active {
      background: var(--brand-blue);
      color: white;
      border-color: var(--brand-blue);
    }

    .panel { display: none; }
    .panel.active { display: block; }

    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }

    .card,
    .kpi {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 18px;
      margin-bottom: 16px;
      box-shadow: var(--shadow);
    }

    h2, h3 {
      margin-top: 0;
      color: var(--brand-brown);
      letter-spacing: -.03em;
    }

    .muted {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }

    .help {
      color: var(--muted);
      font-size: 12px;
      margin-top: -6px;
      margin-bottom: 10px;
      line-height: 1.45;
    }

    button,
    input,
    select,
    textarea {
      padding: 11px 12px;
      border-radius: 14px;
      border: 1px solid #d8d4e5;
      box-sizing: border-box;
      font-size: 14px;
      width: 100%;
      font-family: inherit;
    }

    input,
    select,
    textarea {
      background: white;
      color: var(--brand-brown);
    }

    button {
      cursor: pointer;
      background: var(--brand-blue);
      color: white;
      border: none;
      font-weight: 900;
    }

    button.secondary {
      background: var(--soft-blue);
      color: var(--brand-blue);
      border: 1px solid rgba(54,51,210,.20);
    }

    button.success {
      background: var(--brand-brown);
      color: white;
    }

    .row {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .row > * { flex: 1; }

    .kpis {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
    }

    .kpi {
      margin-bottom: 0;
      position: relative;
      overflow: hidden;
    }

    .kpi::after {
      content: "";
      position: absolute;
      right: -20px;
      top: -20px;
      width: 80px;
      height: 80px;
      border-radius: 50%;
      background: rgba(255,255,102,.45);
    }

    .kpi .v {
      position: relative;
      z-index: 1;
      margin-top: 4px;
      font-size: 34px;
      font-weight: 950;
      color: var(--brand-blue);
      letter-spacing: -.05em;
    }

    .table-wrap { overflow: auto; }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    th,
    td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }

    th {
      background: #faf9ff;
      color: var(--brand-brown);
      position: sticky;
      top: 0;
      z-index: 2;
    }

    .pill {
      display: inline-block;
      background: var(--soft-blue);
      color: var(--brand-blue);
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
      font-weight: 850;
    }

    .preview-box {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      min-height: 220px;
      white-space: normal;
      line-height: 1.45;
    }

    .result {
      white-space: pre-wrap;
      background: var(--brand-brown);
      color: #fff;
      border-radius: 16px;
      padding: 12px;
      font-size: 13px;
      min-height: 140px;
      margin-top: 12px;
      overflow: auto;
      max-height: 420px;
    }

    details summary {
      cursor: pointer;
      font-weight: 900;
      color: var(--brand-blue);
    }

    .empty {
      padding: 18px;
      border: 1px dashed #c9c3d9;
      border-radius: 18px;
      background: #fbfaff;
      color: var(--muted);
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 12px;
      margin-bottom: 18px;
    }

    .summary-card {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
    }

    .summary-card span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
    }

    .summary-card b {
      color: var(--brand-brown);
      font-size: 18px;
    }

    .client-block {
      margin-top: 18px;
    }

    .client-title {
      font-weight: 950;
      margin-bottom: 12px;
      color: var(--brand-brown);
    }

    .timeline-row {
      display: grid;
      grid-template-columns: 46px 1fr;
      gap: 12px;
      margin-bottom: 12px;
    }

    .timeline-dot {
      width: 38px;
      height: 38px;
      border-radius: 14px;
      display: grid;
      place-items: center;
      background: var(--brand-blue);
      color: white;
      font-weight: 950;
      box-shadow: 0 10px 22px rgba(54,51,210,.20);
    }

    .timeline-card {
      background: white;
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 16px;
    }

    .timeline-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }

    .timeline-head b {
      display: block;
      font-size: 17px;
      color: var(--brand-brown);
    }

    .timeline-head p {
      margin: 4px 0 0;
      color: var(--muted);
    }

    .badge {
      white-space: nowrap;
      background: var(--soft-yellow);
      color: var(--brand-brown);
      border: 1px solid rgba(40,12,16,.12);
      border-radius: 999px;
      padding: 7px 10px;
      font-size: 12px;
      font-weight: 900;
    }

    .meta-line {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 12px;
      color: var(--muted);
      font-size: 13px;
    }

    .meta-line span {
      background: #fbfaff;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 6px 9px;
    }

    .content-list {
      margin-top: 12px;
      display: grid;
      gap: 8px;
    }

    .content-chip {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      background: #fbfaff;
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 10px 12px;
      font-weight: 800;
    }

    .content-chip small {
      color: var(--brand-blue);
      font-weight: 800;
    }

    @media (max-width: 980px) {
      .grid,
      .kpis,
      .summary-grid {
        grid-template-columns: 1fr;
      }

      .timeline-row {
        grid-template-columns: 1fr;
      }

      .timeline-dot {
        display: none;
      }
    }
  </style>
</head>

<body>
  <header>
    <div class="brand">
      <img
        class="brand-logo"
        src="URL_DU_LOGO_MOBISUR"
        alt="MobiSûr – Prévention routière des PME"
      />
    
      <div class="brand-copy">
        <div class="muted" style="margin-top:8px;">
          Pilotage des envois et programmation des scénarios
        </div>
      </div>
    </div>
  </header>

  <main>
    <div class="tabs">
      <button type="button" onclick="showPanel('auto')" id="tabAuto" class="tab active">Suivi des envois</button>
      <button type="button" onclick="showPanel('manual')" id="tabManual" class="tab">Programmer un scénario</button>
    </div>

    <div id="panelAuto" class="panel active">
      <div class="kpis">
        <div class="kpi">
          <div class="muted">Envois planifiés</div>
          <div class="v" id="kpiJobs">0</div>
        </div>
        <div class="kpi">
          <div class="muted">À envoyer</div>
          <div class="v" id="kpiReady">0</div>
        </div>
        <div class="kpi">
          <div class="muted">Envoyés</div>
          <div class="v" id="kpiSent">0</div>
        </div>
        <div class="kpi">
          <div class="muted">Emails créés</div>
          <div class="v" id="kpiEmails">0</div>
        </div>
      </div>

      <div class="card" style="margin-top:16px;">
        <h2>Envois planifiés</h2>
        <p class="muted">Liste des messages programmés par client, scénario et moment d’envoi.</p>

        <div class="row" style="margin-bottom:12px;">
          <input id="jobsFilter" placeholder="Filtrer par client, scénario, statut..." />
          <button type="button" class="secondary" onclick="reloadJobsAndEmails()">Rafraîchir</button>
          <button type="button" class="secondary" onclick="processDue()">Envoyer les messages prêts</button>
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
        <p class="muted">Emails réellement créés après agrégation des messages dus par client et par jour.</p>

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
          <h2>1. Choisir le scénario</h2>
          <select id="scenarioSelect"></select>
          <div class="help">Choisissez le scénario que vous souhaitez lancer manuellement.</div>
          <div id="scenarioInfo" class="preview-box"></div>
        </div>

        <div class="card">
          <h2>2. Choisir les destinataires</h2>
          <select id="targetMode">
            <option value="all">Tous les clients actifs</option>
            <option value="zone">Clients d’une zone</option>
          </select>
          <input id="targetZone" placeholder="Ex. 56" style="display:none;margin-top:10px;" />
          <div class="help">En mode manuel, l’interface crée un événement technique caché de type MANUAL_TRIGGER.</div>
          <div id="clientSummary" class="preview-box"></div>
        </div>
      </div>

      <div class="grid">
        <div class="card">
          <h2>3. Date de référence</h2>
          <input id="startAt" type="datetime-local" />
          <div class="help">Cette date sert de référence pour calculer la programmation de chaque étape.</div>
        </div>

        <div class="card">
          <h2>4. Simuler ou programmer</h2>
          <div class="row">
            <button type="button" class="secondary" onclick="simulateManual()">Simuler</button>
            <button type="button" onclick="launchManual(false)">Programmer</button>
            <button type="button" class="success" onclick="launchManual(true)">Programmer et envoyer ce qui est dû</button>
          </div>
          <div class="help">La simulation montre les envois qui seraient programmés, sans modifier la base.</div>
        </div>
      </div>

      <div class="card">
        <h2>Séquence prévue</h2>
        <div class="help">Chaque carte correspond à un moment d’envoi. Une carte peut regrouper plusieurs contenus.</div>
        <div id="manualPreview" class="preview-box">Lancez une simulation pour voir le détail.</div>
      </div>

      <details class="card">
        <summary>Détails techniques</summary>
        <div id="result" class="result">Aucune action exécutée.</div>
      </details>
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

    function escapeHtml(value) {
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
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
        document.getElementById('result').textContent =
          'Erreur au chargement :\\n\\n' + (e.message || String(e));
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

      var steps = await fetch('/api/scenarios/' + id + '/steps').then(function(r) {
        return r.json();
      });

      var lines = [];
      lines.push('Scénario sélectionné');
      lines.push('');
      lines.push('Nom : ' + sc.label);
      lines.push('Code : ' + sc.code);
      lines.push('Mode : ' + sc.aggregation_mode);
      lines.push('Priorité : ' + sc.priority);
      lines.push('');
      lines.push('Ordre des moments d’envoi :');

      if (Array.isArray(steps) && steps.length) {
        steps.forEach(function(s, idx) {
          var contents = s.logic_json && Array.isArray(s.logic_json.contents)
            ? s.logic_json.contents.length
            : 0;

          lines.push(
            (idx + 1) + '. Étape ' + s.code +
            ' | référence : ' + translateWindowRef(s.window_ref) +
            ' | envoi : ' + s.window_max_hours + 'h avant' +
            ' | contenus : ' + contents
          );
        });
      } else {
        lines.push('Aucune étape active');
      }

      document.getElementById('scenarioInfo').textContent = lines.join('\\n');
    }

    function toggleTargetInputs() {
      var mode = document.getElementById('targetMode').value;
      document.getElementById('targetZone').style.display = mode === 'zone' ? 'block' : 'none';
      loadClientSummary();
    }

    async function loadClientSummary() {
      var data = await fetch('/api/clients/summary').then(function(r) {
        return r.json();
      });

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

      document.getElementById('clientSummary').textContent = lines.join('\\n');
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
        box.innerHTML = '<div class="empty">Aucune prévisualisation disponible.</div>';
        return;
      }

      if (!data.preview.length) {
        box.innerHTML = '<div class="empty">Aucun envoi ne serait programmé pour la cible choisie.</div>';
        return;
      }

      var groupedByClient = {};

      data.preview.forEach(function(item) {
        var key = item.client_email || ('client-' + item.client_id);
        if (!groupedByClient[key]) groupedByClient[key] = [];
        groupedByClient[key].push(item);
      });

      var html = '';

      html += '<div class="summary-grid">';
      html += '<div class="summary-card"><span>Scénario</span><b>' + escapeHtml(data.scenario_label || '') + '</b></div>';
      html += '<div class="summary-card"><span>Clients concernés</span><b>' + escapeHtml(data.clients_concernes || 0) + '</b></div>';
      html += '<div class="summary-card"><span>Envois programmés</span><b>' + escapeHtml(data.messages_programmes || 0) + '</b></div>';
      html += '</div>';

      Object.keys(groupedByClient).forEach(function(clientEmail) {
        var items = groupedByClient[clientEmail].slice().sort(function(a, b) {
          return new Date(a.planned_send_at).getTime() - new Date(b.planned_send_at).getTime();
        });

        html += '<div class="client-block">';
        html += '<div class="client-title">Destinataire : ' + escapeHtml(clientEmail) + '</div>';

        items.forEach(function(item, index) {
          var grouped = item.is_grouped_step;

          var typeLabel = grouped
            ? 'Regroupement de ' + item.content_count + ' contenus'
            : 'Message simple';

          html += '<div class="timeline-row">';
          html += '<div class="timeline-dot">' + escapeHtml(index + 1) + '</div>';
          html += '<div class="timeline-card">';
          html += '<div class="timeline-head">';
          html += '<div>';
          html += '<b>' + escapeHtml(item.step_code || 'Étape') + '</b>';
          html += '<p>' + escapeHtml(item.subject_rendered || 'Sans objet') + '</p>';
          html += '</div>';
          html += '<span class="badge">' + escapeHtml(typeLabel) + '</span>';
          html += '</div>';

          html += '<div class="meta-line">';
          html += '<span>Envoi prévu : <strong>' + escapeHtml(formatDateFr(item.planned_send_at)) + '</strong></span>';
          html += '<span>Calcul : ' + escapeHtml(translateWindowRef(item.step_window_ref)) + ' · ' + escapeHtml(item.step_window_max_hours) + 'h avant</span>';
          html += '</div>';

          if (Array.isArray(item.contents) && item.contents.length) {
            html += '<div class="content-list">';

            item.contents.forEach(function(content) {
              html += '<div class="content-chip">';
              html += escapeHtml(content.content_title || content.content_code);
              html += '<small>' + escapeHtml(content.channel || 'email') + '</small>';
              html += '</div>';
            });

            html += '</div>';
          }

          html += '</div>';
          html += '</div>';
        });

        html += '</div>';
      });

      box.innerHTML = html;
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
      var rows = await fetch('/api/jobs').then(function(r) {
        return r.json();
      });

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
            '<td>' + escapeHtml(r.id) + '</td>' +
            '<td>' + escapeHtml(r.client_email || r.client_id) + '</td>' +
            '<td>' + escapeHtml(r.scenario_label || r.scenario_code || r.scenario_id) + '</td>' +
            '<td>' + escapeHtml(r.step_code || r.scenario_step_id) + '</td>' +
            '<td>' + escapeHtml(r.planned_send_at || '') + '</td>' +
            '<td>' + escapeHtml(r.subject_rendered || '') + '</td>' +
            '<td><span class="pill">' + escapeHtml(r.status || '') + '</span></td>';

          body.appendChild(tr);
        });
    }

    async function reloadEmails() {
      var rows = await fetch('/api/outbound-emails').then(function(r) {
        return r.json();
      });

      emailsData = Array.isArray(rows) ? rows : [];

      var body = document.getElementById('emailsBody');
      body.innerHTML = '';

      emailsData.forEach(function(r) {
        var tr = document.createElement('tr');

        tr.innerHTML =
          '<td>' + escapeHtml(r.id) + '</td>' +
          '<td>' + escapeHtml(r.client_email || r.client_id) + '</td>' +
          '<td>' + escapeHtml(r.send_date || '') + '</td>' +
          '<td>' + escapeHtml(r.subject_rendered || '') + '</td>' +
          '<td><span class="pill">' + escapeHtml(r.status || '') + '</span></td>';

        body.appendChild(tr);
      });

      computeKpis();
    }

    async function processDue() {
      try {
        var response = await fetch('/api/process-due', { method: 'POST' });
        var text = await response.text();

        console.log('HTTP status /api/process-due =', response.status);
        console.log('Réponse brute /api/process-due =', text);

        var data;

        try {
          data = JSON.parse(text);
        } catch (e) {
          throw new Error('La réponse n’est pas un JSON valide : ' + text);
        }

        document.getElementById('result').textContent = JSON.stringify(data, null, 2);

        if (!response.ok) {
          throw new Error(data.error || 'Erreur HTTP ' + response.status);
        }

        await reloadJobsAndEmails();
      } catch (e) {
        console.error('Erreur processDue()', e);

        document.getElementById('result').textContent =
          'Erreur lors du traitement des envois dus :\\n\\n' + (e.message || String(e));
      }
    }

    boot();
  </script>
</body>
</html>`;
}
