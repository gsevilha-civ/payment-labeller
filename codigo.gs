/*************************************************************
 * Fraud Risk — Payment attempts (día calendario anterior, TZ Madrid) — TRAFFIC-ALIGNED (FULL, TOP10 DEBUGGED)
 *
 * WHAT THIS SCRIPT DOES:
 *  - READ-ONLY monitoring tool
 *  - Queries BigQuery for payment transaction data
 *  - Calculates fraud risk scores based on booking window, catalog risk, country risk
 *  * Filters payment_transaction to payment_type = 'pay'
 *  - DOES NOT create new payment authorizations or process payments
 *  - DOES NOT write, update, or delete any data
 *
 * FIXES:
 *  ✅ Enhanced JSON parsing with multiple fallback strategies
 *  ✅ Better error handling and diagnostic logging
 *  ✅ Improved BigQuery result structure with explicit CASTs
 *  ✅ Detailed logging for debugging top10_json parsing issues
 *  ✅ Added payment_type filter consistently in main query + fallback query
 *************************************************************/

const CFG = {
  PROJECT_ID: "data-mart-370422",
  DATASET_ID: "dbt__data_mart",

  PAYMENTS_TABLE: "data-mart-370422.dbt__data_mart.payment_transaction",
  RESERVAS_TABLE: "data-mart-370422.dbt__data_mart.reservas",
  CATALOGO_TABLE: "data-mart-370422.dbt__data_mart.catalogo",
  DESTINOS_TABLE: "data-mart-370422.dbt__data_mart.destinos",
  PAISES_TABLE: "data-mart-370422.dbt__data_mart.paises",
  ACP_TABLE: "data-mart-370422.dbt__data_mart.activity_cancel_policy",

  PAYMENT_TS_FIELD: "created_at", // DATETIME
  PAYMENT_TYPE_FILTER: "Pay",
  /** Ventana: día calendario anterior completo en TIMEZONE (ya no últimos N minutos). */
  TOP_N: 10,

  EXCLUDE_TYPOLOGY: 1, // null to disable
  CANAL_AGENCIA_RISK_REDUCTION: 0.5, // Multiplier for risk score when canal="Agencia" (0.5 = 50% reduction = safer)

  W_BOOKING_WINDOW: 0.35,
  W_CATALOG_RISK: 0.40,
  W_COUNTRY_RISK: 0.25,

  TIMEZONE: "Europe/Madrid",

  /** Hoja donde se escribe el resumen analítico y el gráfico (script vinculado a Sheets). */
  ANALYTICS_SHEET: "Analítica riesgo",
  /** Origen diario para el histórico de sparklines (misma tabla «Categoría» / «Intentos» que la analítica). */
  DASHBOARD_DATA_SHEET: "dashboard_data",
  /** Append-only: Fecha, Categoría, Porcentaje, Intentos (rellena `recordDailyTraffic`). */
  HISTORICO_TRAFFIC_SHEET: "Historico_Traffic"
};

/* =========================
   MAIN
========================= */

function runFraudRiskPayments30mAlert() {
  const startTime = Date.now();
  const runTsIso = new Date().toISOString();

  try {
    Logger.log(`[START] Fraud risk job - ${runTsIso}`);

    const location = getDatasetLocation_(CFG.PROJECT_ID, CFG.DATASET_ID);
    Logger.log(`[INFO] Dataset location: ${location}`);

    const sql = buildSQL_();
    Logger.log(`[INFO] Executing BigQuery...`);

    const result = runBigQuerySingleRow_(CFG.PROJECT_ID, location, sql);
    if (!result) throw new Error("Query returned no results");

    const parseJSONSafe_ = (value, label) => {
      if (value === null || value === undefined || value === "") {
        Logger.log(`[DEBUG] ${label}: value is null/undefined/empty`);
        return null;
      }
      if (typeof value === "object") {
        Logger.log(`[DEBUG] ${label}: value is already an object, type: ${Array.isArray(value) ? 'array' : 'object'}`);
        return value;
      }
      if (typeof value !== "string") {
        Logger.log(`[DEBUG] ${label}: value is not a string, type: ${typeof value}`);
        return null;
      }

      let s = value.trim();

      if (s === "null" || s === "NULL" || s === '""' || s === "''") {
        Logger.log(`[DEBUG] ${label}: value is string "null" or empty quotes`);
        return null;
      }
      if (!s) {
        Logger.log(`[DEBUG] ${label}: value is empty after trim`);
        return null;
      }

      let attempts = 0;
      let currentValue = s;

      while (attempts < 3) {
        try {
          const parsed = JSON.parse(currentValue);

          if (typeof parsed === "string" && attempts < 2) {
            Logger.log(`[DEBUG] ${label}: Parsed to string (likely double-encoded), attempting to parse again (attempt ${attempts + 1})`);
            currentValue = parsed;
            attempts++;
            continue;
          }

          if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            if (parsed.arr && Array.isArray(parsed.arr)) {
              Logger.log(`[DEBUG] ${label}: Extracted array from object wrapper`);
              return parsed.arr;
            }
            const keys = Object.keys(parsed).filter(k => !isNaN(Number(k))).map(Number).sort((a, b) => a - b);
            if (keys.length > 0) {
              Logger.log(`[DEBUG] ${label}: Converting object with numeric keys to array`);
              return keys.map(k => parsed[k]);
            }
          }

          Logger.log(`[DEBUG] ${label}: Successfully parsed (attempt ${attempts + 1}), type: ${Array.isArray(parsed) ? 'array' : typeof parsed}, length: ${Array.isArray(parsed) ? parsed.length : 'N/A'}`);
          return parsed;
        } catch (e) {
          if (attempts === 0) {
            const arrayStart = currentValue.indexOf('[');
            const arrayEnd = currentValue.lastIndexOf(']');
            if (arrayStart >= 0 && arrayEnd > arrayStart) {
              try {
                const extracted = currentValue.substring(arrayStart, arrayEnd + 1);
                Logger.log(`[DEBUG] ${label}: Attempting to extract JSON array from string (chars ${arrayStart}-${arrayEnd})`);
                const parsed = JSON.parse(extracted);
                Logger.log(`[DEBUG] ${label}: Successfully extracted and parsed`);
                return parsed;
              } catch (e2) {
                Logger.log(`[WARN] ${label}: Extraction also failed: ${e2.message}`);
              }
            }
          }

          if (attempts > 0) {
            Logger.log(`[WARN] ${label}: Parse failed after ${attempts + 1} attempts: ${e.message}`);
            Logger.log(`[WARN] ${label} preview (first 500 chars): ${currentValue.substring(0, 500)}`);
            Logger.log(`[WARN] ${label} full length: ${currentValue.length}`);
            return null;
          }

          if (currentValue.startsWith('"') && currentValue.endsWith('"') && currentValue.length > 2) {
            try {
              currentValue = JSON.parse(currentValue);
              attempts++;
              continue;
            } catch (e3) {
              Logger.log(`[WARN] ${label}: Unquoting failed: ${e3.message}`);
            }
          }

          Logger.log(`[WARN] JSON parse failed for ${label}: ${e.message}`);
          Logger.log(`[WARN] ${label} preview (first 500 chars): ${currentValue.substring(0, 500)}`);
          Logger.log(`[WARN] ${label} full length: ${currentValue.length}`);
          return null;
        }
      }

      return null;
    };

    const ensureArray_ = (x) => Array.isArray(x) ? x : [];

    const top10Len = Number(result.top10_len || 0);

    let top = [];

    Logger.log(`[DEBUG] total_attempts(raw): ${result.total_attempts}`);
    Logger.log(`[DEBUG] top10_len(raw): ${result.top10_len} -> ${top10Len}`);

    const top10Arr = result.top10_arr;
    Logger.log(`[DEBUG] top10_arr type=${typeof top10Arr}`);
    Logger.log(`[DEBUG] top10_arr is array=${Array.isArray(top10Arr)}`);
    Logger.log(`[DEBUG] top10_arr is null/undefined=${top10Arr === null || top10Arr === undefined}`);

    if (top10Arr) {
      if (Array.isArray(top10Arr)) {
        top = top10Arr;
        Logger.log(`[DEBUG] ✅ top10_arr is already an array with ${top.length} items`);
      } else if (typeof top10Arr === 'object') {
        if (top10Arr.arr && Array.isArray(top10Arr.arr)) {
          top = top10Arr.arr;
          Logger.log(`[DEBUG] ✅ Extracted array from top10_arr.arr: ${top.length} items`);
        } else if (top10Arr.v && Array.isArray(top10Arr.v)) {
          top = top10Arr.v;
          Logger.log(`[DEBUG] ✅ Extracted array from top10_arr.v: ${top.length} items`);
        } else {
          const keys = Object.keys(top10Arr).filter(k => !isNaN(Number(k))).map(Number).sort((a, b) => a - b);
          if (keys.length > 0) {
            top = keys.map(k => top10Arr[k]).filter(x => x !== undefined);
            Logger.log(`[DEBUG] ✅ Converted object with numeric keys to array: ${top.length} items`);
          } else {
            top = [top10Arr];
            Logger.log(`[DEBUG] ⚠️ Treated object as single-item array`);
          }
        }
      } else if (typeof top10Arr === 'string') {
        Logger.log(`[DEBUG] top10_arr is a string, attempting JSON parse`);
        const topParsed = ensureArray_(parseJSONSafe_(top10Arr, "top10_arr"));
        top = topParsed;
      }
    }

    if (top.length === 0 && result.top10_json) {
      Logger.log(`[DEBUG] Fallback 1: trying top10_json field`);
      const topRaw = result.top10_json;
      const topParsed = ensureArray_(parseJSONSafe_(topRaw, "top10_json"));
      top = topParsed;
    }

    const hasValidData = top.length > 0 && top.some(item => item && (item.risk_score !== undefined || item.pnr !== undefined));

    if ((top.length === 0 || !hasValidData) && top10Len > 0) {
      Logger.log(`[WARN] ⚠️ top10_len=${top10Len} but array is empty or has no valid data. Attempting fallback query...`);
      Logger.log(`[WARN] Array length: ${top.length}, Has valid data: ${hasValidData}`);
      try {
        const fallbackTop = fetchTop10Fallback_(CFG.PROJECT_ID, location, result.window_start_dt, result.window_end_dt);
        if (fallbackTop && fallbackTop.length > 0) {
          top = fallbackTop;
          Logger.log(`[DEBUG] ✅ Fallback query retrieved ${top.length} items`);
          const validItems = top.filter(item => item && (item.risk_score !== undefined || item.pnr !== undefined));
          Logger.log(`[DEBUG] Fallback items with valid data: ${validItems.length} out of ${top.length}`);
        } else {
          Logger.log(`[WARN] Fallback query returned empty or null`);
        }
      } catch (e) {
        Logger.log(`[WARN] Fallback query failed: ${e.message}`);
        Logger.log(`[WARN] Fallback error stack: ${e.stack}`);
      }
    }

    Logger.log(`[DEBUG] Final top length=${top.length}`);

    if (top.length > 0) {
      Logger.log(`[DEBUG] First item type: ${typeof top[0]}`);
      Logger.log(`[DEBUG] First item is object: ${typeof top[0] === 'object' && top[0] !== null}`);
      if (top[0] && typeof top[0] === 'object') {
        Logger.log(`[DEBUG] First item keys: ${Object.keys(top[0]).join(', ')}`);
        Logger.log(`[DEBUG] First item full: ${JSON.stringify(top[0]).substring(0, 500)}`);

        const firstItemKeys = Object.keys(top[0]);
        if (firstItemKeys.length === 0) {
          Logger.log(`[WARN] ⚠️ First item has no keys! Array structure may be incorrect.`);
        }
      }
    }

    if (top10Len > 0 && top.length === 0) {
      Logger.log(`[WARN] ⚠️ top10_len=${top10Len} but parsed top is empty!`);
      Logger.log(`[WARN] top10_arr structure: ${JSON.stringify(top10Arr).substring(0, 500)}`);
      if (result.top10_json) {
        Logger.log(`[WARN] top10_json fallback available: ${String(result.top10_json).substring(0, 200)}`);
      }
    } else if (top10Len === top.length && top.length > 0) {
      Logger.log(`[DEBUG] ✅ Successfully extracted ${top.length} top attempts`);
      const itemsWithData = top.filter(item => item && (item.risk_score !== undefined || item.pnr !== undefined));
      Logger.log(`[DEBUG] Items with data: ${itemsWithData.length} out of ${top.length}`);
      if (itemsWithData.length === 0 && top.length > 0) {
        Logger.log(`[WARN] ⚠️ Array has ${top.length} items but none have data!`);
      }
    } else if (top.length > 0) {
      Logger.log(`[DEBUG] ⚠️ Length mismatch: top10_len=${top10Len}, extracted=${top.length}`);
    }

    const execTimeMs = Date.now() - startTime;

    Logger.log(
      `[SUCCESS] ${execTimeMs}ms | BQ ${location} | window ${result.window_start_dt} → ${result.window_end_dt} | ` +
        `attempts=${result.total_attempts} OK=${result.ok_attempts} KO=${result.ko_attempts} | ` +
        `risk CRITICAL=${result.critical_count} HIGH=${result.high_count} MEDIUM=${result.medium_count} LOW=${result.low_count} | ` +
        `top10=${top.length}/${top10Len}`
    );

    return { success: true, attempts: Number(result.total_attempts || 0), execTimeMs };

  } catch (error) {
    const execTimeMs = Date.now() - startTime;
    Logger.log(`[ERROR] ${error.toString()}`);
    handleError_(error, runTsIso, execTimeMs);
    throw error;
  }
}

function testFraudRiskPayments30mAlert() {
  runFraudRiskPayments30mAlert();
}

/* =========================
   WEB APP (HtmlService)
========================= */

/**
 * Serves dashboard.html as a web app (Deploy → New deployment → Web app).
 */
function doGet() {
  return HtmlService.createHtmlOutputFromFile("dashboard")
    .setTitle("Traffic & authorization monitor")
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Runs the same BigQuery window as the risk job and returns risk-tier
 * shares for the dashboard (serializable for google.script.run).
 *
 * Mapping (UI):
 *  Legítimo     = LOW      🟢
 *  Dudoso       = MEDIUM   🟡
 *  Poco fiable  = HIGH     🟠
 *  Malicioso    = CRITICAL 🔴
 */
function getDashboardData() {
  const location = getDatasetLocation_(CFG.PROJECT_ID, CFG.DATASET_ID);
  const sql = buildSQL_();
  const result = runBigQuerySingleRow_(CFG.PROJECT_ID, location, sql);
  if (!result || Object.keys(result).length === 0) {
    throw new Error("Query returned no results");
  }
  return buildDashboardPayloadFromResult_(result);
}

/**
 * Misma forma que getDashboardData(), pero leyendo la pestaña `CFG.ANALYTICS_SHEET`
 * tras `escribirAnaliticaEnHoja()` / `writeAnalyticsToSheet_` (sin ejecutar BigQuery).
 * Pensado para el modal del libro: una sola query, datos y gráfico en hoja, UI desde celdas.
 * Cada elemento de `traffic` incluye `spark`: últimos 30 % leídos de `CFG.HISTORICO_TRAFFIC_SHEET`
 * por categoría (relleno con el % actual si hay menos de 30 días).
 */
function getDashboardDataFromSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) throw new Error("No hay libro activo.");

  const sheetName = CFG.ANALYTICS_SHEET || "Analítica riesgo";
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    throw new Error('No existe la hoja «' + sheetName + '». Ejecuta primero «Solo actualizar hoja analítica» o «Abrir Monitor».');
  }

  const summaryStart = findAnalyticsSummaryStartRow_(sheet);
  /** Debe coincidir con el número de filas del array `summary` en writeAnalyticsToSheet_. */
  const SUMMARY_ROW_COUNT = 14;
  const summaryVals = summaryStart
    ? sheet.getRange(summaryStart, 2, SUMMARY_ROW_COUNT, 1).getValues()
    : [];

  const tierBlock = findAnalyticsTierTable_(sheet);
  if (!tierBlock) {
    throw new Error(
      "No se encontró la tabla de tiers (cabecera «Categoría» / «Intentos»). Actualiza la hoja analítica."
    );
  }

  const tierRows = readAnalyticsTierTableRows_(sheet, tierBlock);
  const traffic = [];
  for (let i = 0; i < tierRows.length; i++) {
    const label = tierRows[i][0];
    if (label === "" || label === null) break;
    const count = tierRows[i][1];
    const tier = tierRows[i][2];
    const pct = tierRows[i][3];
    traffic.push(buildTrafficItemFromSheetRow_(String(label), count, String(tier || ""), pct));
  }

  if (traffic.length === 0) {
    throw new Error("La tabla de tiers en la hoja está vacía. Actualiza la analítica.");
  }

  for (let ti = 0; ti < traffic.length; ti++) {
    traffic[ti].spark = getSparkSeries30DaysFromHistorico_(ss, traffic[ti].label, traffic[ti].pct);
  }

  const g = (idx) => (summaryVals[idx] && summaryVals[idx][0] !== "" ? summaryVals[idx][0] : "");
  const num = (idx) => {
    const v = summaryVals[idx] ? summaryVals[idx][0] : "";
    if (v === "" || v === null || v === undefined) return 0;
    return Number(v);
  };

  return {
    run_anchor_dt: String(g(0)),
    window_start_dt: String(g(1)),
    window_end_dt: String(g(2)),
    window_minutes: num(3),
    total_attempts: num(4),
    critical_count: traffic.filter((t) => t.tier === "CRITICAL").reduce((s, t) => s + t.count, 0),
    high_count: traffic.filter((t) => t.tier === "HIGH").reduce((s, t) => s + t.count, 0),
    medium_count: traffic.filter((t) => t.tier === "MEDIUM").reduce((s, t) => s + t.count, 0),
    low_count: traffic.filter((t) => t.tier === "LOW").reduce((s, t) => s + t.count, 0),
    traffic: traffic
  };
}

function findAnalyticsSummaryStartRow_(sheet) {
  const last = Math.min(Math.max(sheet.getLastRow(), 1), 80);
  const labels = sheet.getRange(1, 1, last, 1).getValues();
  for (let i = 0; i < labels.length; i++) {
    const a = String(labels[i][0] || "");
    if (a === "Referencia datos (BQ)") return i + 1;
  }
  return 0;
}

function findAnalyticsTierTable_(sheet) {
  const last = Math.min(Math.max(sheet.getLastRow(), 1), 120);
  const block = sheet.getRange(1, 1, last, 4).getValues();
  for (let r = 0; r < block.length; r++) {
    const c0 = String(block[r][0] || "").trim();
    const c1 = String(block[r][1] || "").trim();
    if (c0 === "Categoría" && c1 === "Intentos") {
      return { headerRow: r + 1, dataStartRow: r + 2 };
    }
  }
  return null;
}

/**
 * Filas de la tabla «Categoría» / «Intentos» (máx. 8 filas desde `dataStartRow`).
 * `Sheet.getRange(fila, col, numFilas, numCols)` usa conteos, no fila final.
 */
function readAnalyticsTierTableRows_(sheet, tierBlock) {
  const lastRow = sheet.getLastRow();
  if (lastRow < tierBlock.dataStartRow) return [];
  const tierEndRow = Math.min(tierBlock.dataStartRow + 7, lastRow);
  const numRows = tierEndRow - tierBlock.dataStartRow + 1;
  return sheet.getRange(tierBlock.dataStartRow, 1, numRows, 4).getValues();
}

/**
 * Añade filas a `CFG.HISTORICO_TRAFFIC_SHEET` desde la tabla de tiers de `src`
 * (cabecera «Categoría» / «Intentos»). Usado por el activador diario y al abrir el monitor.
 */
function appendHistoricoTrafficSnapshotFromSheet_(ss, src, srcLabelForErrors) {
  const srcName = srcLabelForErrors || (src && src.getName()) || "hoja";
  if (!ss) throw new Error("No hay libro activo.");
  if (!src) throw new Error("Hoja origen inválida para el histórico.");

  const tierBlock = findAnalyticsTierTable_(src);
  if (!tierBlock) {
    throw new Error(
      "No se encontró la tabla de tiers en «" + srcName + "» (cabecera «Categoría» / «Intentos»)."
    );
  }

  const histName = CFG.HISTORICO_TRAFFIC_SHEET || "Historico_Traffic";
  let hist = ss.getSheetByName(histName);
  if (!hist) {
    hist = ss.insertSheet(histName);
  }
  if (hist.getLastRow() === 0) {
    hist
      .getRange(1, 1, 1, 4)
      .setValues([["Fecha", "Categoría", "Porcentaje", "Intentos"]])
      .setFontWeight("bold")
      .setBackground("#f3f3f3");
  }

  const now = new Date();
  const tierRows = readAnalyticsTierTableRows_(src, tierBlock);
  const rowsToAppend = [];
  for (let i = 0; i < tierRows.length; i++) {
    const label = tierRows[i][0];
    if (label === "" || label === null) break;
    const count = tierRows[i][1];
    const pctCell = tierRows[i][3];
    const c = count === "" || count === null || count === undefined ? 0 : Number(count);
    let p = 0;
    if (pctCell !== "" && pctCell !== null && pctCell !== undefined) {
      p = typeof pctCell === "number" ? pctCell : Number(String(pctCell).replace(",", "."));
    }
    if (isNaN(p)) p = 0;
    const pRounded = Math.round(p * 10) / 10;
    rowsToAppend.push([now, String(label), pRounded, isNaN(c) ? 0 : c]);
  }

  if (rowsToAppend.length === 0) {
    throw new Error("La tabla de tiers en «" + srcName + "» no tiene filas para registrar.");
  }

  const startRow = hist.getLastRow() + 1;
  const nAppend = rowsToAppend.length;
  hist.getRange(startRow, 1, nAppend, 4).setValues(rowsToAppend);
  hist.getRange(startRow, 1, nAppend, 1).setNumberFormat("yyyy-mm-dd hh:mm");
  hist.getRange(startRow, 3, nAppend, 1).setNumberFormat("0.0");
  hist.getRange(startRow, 4, nAppend, 1).setNumberFormat("#,##0");
}

/**
 * Copia un snapshot diario de la tabla de tiers de `CFG.DASHBOARD_DATA_SHEET`
 * a `CFG.HISTORICO_TRAFFIC_SHEET` (Fecha, Categoría, Porcentaje, Intentos).
 * Pensado para activador horario diario.
 */
function recordDailyTraffic() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) throw new Error("No hay libro activo.");

  const srcName = CFG.DASHBOARD_DATA_SHEET || "dashboard_data";
  const src = ss.getSheetByName(srcName);
  if (!src) {
    throw new Error('No existe la hoja «' + srcName + '». Crea la pestaña o ajusta CFG.DASHBOARD_DATA_SHEET.');
  }

  appendHistoricoTrafficSnapshotFromSheet_(ss, src, srcName);
}

function dateKeyInTimezone_(value) {
  const tz = CFG.TIMEZONE || "Europe/Madrid";
  let d = value;
  if (!(d instanceof Date)) {
    if (d === "" || d === null || d === undefined) return "";
    d = new Date(d);
  }
  if (isNaN(d.getTime())) return "";
  return Utilities.formatDate(d, tz, "yyyy-MM-dd");
}

function parsePctCellValue_(pctCell) {
  let p = 0;
  if (pctCell !== "" && pctCell !== null && pctCell !== undefined) {
    p = typeof pctCell === "number" ? pctCell : Number(String(pctCell).replace(",", "."));
  }
  if (isNaN(p)) p = 0;
  return Math.round(p * 10) / 10;
}

/**
 * Últimos 30 % por categoría (una muestra por día calendario en TZ; gana la última fila del día).
 * Rellena por la izquierda con `currentPct` si hay menos de 30 días.
 */
function getSparkSeries30DaysFromHistorico_(ss, label, currentPct) {
  const histName = CFG.HISTORICO_TRAFFIC_SHEET || "Historico_Traffic";
  const sheet = ss.getSheetByName(histName);
  const want = 30;
  const cur = typeof currentPct === "number" && !isNaN(currentPct) ? currentPct : 0;
  if (!sheet || sheet.getLastRow() < 2) {
    const flat = [];
    for (let i = 0; i < want; i++) flat.push(cur);
    return flat;
  }

  const lastR = sheet.getLastRow();
  const numHistRows = lastR - 1;
  const data = sheet.getRange(2, 1, numHistRows, 4).getValues();
  const needle = String(label || "").trim();
  const byDay = {};
  for (let r = 0; r < data.length; r++) {
    const cat = String(data[r][1] || "").trim();
    if (cat !== needle) continue;
    const key = dateKeyInTimezone_(data[r][0]);
    if (!key) continue;
    byDay[key] = parsePctCellValue_(data[r][2]);
  }
  const days = Object.keys(byDay).sort();
  const series = days.slice(-want).map((k) => byDay[k]);
  while (series.length < want) series.unshift(cur);
  if (series.length > want) return series.slice(-want);
  return series;
}

function tierUiMeta_(tier) {
  const t = String(tier || "").toUpperCase();
  const map = {
    LOW: { label: "Legítimo", emoji: "🟢", color: "#22c55e" },
    MEDIUM: { label: "Dudoso", emoji: "🟡", color: "#f59e0b" },
    HIGH: { label: "Poco fiable", emoji: "🟠", color: "#c2410c" },
    CRITICAL: { label: "Malicioso", emoji: "🔴", color: "#ef4444" }
  };
  return map[t] || { label: String(tier || ""), emoji: "", color: "#888888" };
}

function buildTrafficItemFromSheetRow_(label, count, tier, pctCell) {
  const meta = tierUiMeta_(tier);
  const c = count === "" || count === null || count === undefined ? 0 : Number(count);
  let p = 0;
  if (pctCell !== "" && pctCell !== null && pctCell !== undefined) {
    p = typeof pctCell === "number" ? pctCell : Number(String(pctCell).replace(",", "."));
  }
  if (isNaN(p)) p = 0;
  return {
    label: label || meta.label,
    tier: tier || "LOW",
    emoji: meta.emoji,
    color: meta.color,
    pct: Math.round(p * 10) / 10,
    count: c,
    cnt: Number(c).toLocaleString("es-ES"),
    yoy: null
  };
}

/**
 * Misma lógica que el modal/HTML: métricas de ventana + tráfico por tier.
 * `traffic[].count` es el valor numérico (para hojas); `cnt` sigue formateado para UI.
 */
function buildDashboardPayloadFromResult_(result) {
  const n = (v) => (v === null || v === undefined || v === "") ? 0 : Number(v);
  const low = n(result.low_count);
  const medium = n(result.medium_count);
  const high = n(result.high_count);
  const critical = n(result.critical_count);
  const totalTier = low + medium + high + critical;

  const pct = (c) => (totalTier > 0 ? Math.round((1000 * c) / totalTier) / 10 : 0);
  const fmtCnt = (c) => Number(c).toLocaleString("es-ES");
  const spark = (p) => {
    const v = pct(p);
    const out = [];
    for (let i = 0; i < 30; i++) out.push(v);
    return out;
  };

  return {
    run_anchor_dt: String(result.run_anchor_dt || ""),
    window_start_dt: String(result.window_start_dt || ""),
    window_end_dt: String(result.window_end_dt || ""),
    window_minutes: n(result.window_minutes),
    total_attempts: n(result.total_attempts),
    critical_count: critical,
    high_count: high,
    medium_count: medium,
    low_count: low,
    traffic: [
      { label: "Legítimo", tier: "LOW", emoji: "🟢", color: "#22c55e", pct: pct(low), count: low, cnt: fmtCnt(low), yoy: null, spark: spark(low) },
      { label: "Dudoso", tier: "MEDIUM", emoji: "🟡", color: "#f59e0b", pct: pct(medium), count: medium, cnt: fmtCnt(medium), yoy: null, spark: spark(medium) },
      { label: "Poco fiable", tier: "HIGH", emoji: "🟠", color: "#c2410c", pct: pct(high), count: high, cnt: fmtCnt(high), yoy: null, spark: spark(high) },
      { label: "Malicioso", tier: "CRITICAL", emoji: "🔴", color: "#ef4444", pct: pct(critical), count: critical, cnt: fmtCnt(critical), yoy: null, spark: spark(critical) }
    ]
  };
}

function parseJsonArraySafe_(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  if (typeof value !== "string") return [];
  try {
    const p = JSON.parse(value);
    return Array.isArray(p) ? p : [];
  } catch (e) {
    return [];
  }
}

/**
 * Ejecuta BigQuery y escribe en la hoja `CFG.ANALYTICS_SHEET`: resumen, tabla por tier,
 * pasarelas (si hay datos) y un gráfico de barras de la distribución por riesgo.
 * Requiere proyecto de Apps Script vinculado a un libro de Google Sheets.
 */
function escribirAnaliticaEnHoja() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) throw new Error("No hay un libro activo. Vincula este script a una hoja de cálculo.");

  const location = getDatasetLocation_(CFG.PROJECT_ID, CFG.DATASET_ID);
  const sql = buildSQL_();
  const result = runBigQuerySingleRow_(CFG.PROJECT_ID, location, sql);
  if (!result || Object.keys(result).length === 0) throw new Error("La consulta no devolvió resultados.");

  writeAnalyticsToSheet_(ss, result);
  ss.toast(
    "Hoja «" + (CFG.ANALYTICS_SHEET || "Analítica riesgo") + "» actualizada (tablas y gráfico).",
    "Analítica",
    5
  );
  return buildDashboardPayloadFromResult_(result);
}

function writeAnalyticsToSheet_(ss, result) {
  const sheetName = CFG.ANALYTICS_SHEET || "Analítica riesgo";
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) sheet = ss.insertSheet(sheetName);

  const existingCharts = sheet.getCharts();
  for (let i = 0; i < existingCharts.length; i++) sheet.removeChart(existingCharts[i]);

  sheet.clear();
  const payload = buildDashboardPayloadFromResult_(result);
  const n = (v) => (v === null || v === undefined || v === "") ? 0 : Number(v);

  sheet.getRange("A1").setValue("Analítica de riesgo — intentos de pago (día anterior, " + (CFG.TIMEZONE || "Europe/Madrid") + ")");
  sheet.getRange("A1:B1").merge().setFontWeight("bold").setFontSize(13);
  sheet.getRange("A2").setValue("Actualizado");
  sheet.getRange("B2").setValue(new Date());
  sheet.getRange("B2").setNumberFormat("yyyy-mm-dd hh:mm:ss");

  const summary = [
    ["Referencia datos (BQ)", payload.run_anchor_dt],
    ["Inicio ventana", payload.window_start_dt],
    ["Fin ventana", payload.window_end_dt],
    ["Ventana (minutos)", payload.window_minutes],
    ["Intentos totales", n(result.total_attempts)],
    ["Resultado OK", n(result.ok_attempts)],
    ["Resultado KO", n(result.ko_attempts)],
    ["Otros resultados", n(result.other_attempts)],
    ["PNRs únicos", n(result.unique_pnrs)],
    ["Riesgo medio", n(result.avg_risk_score)],
    ["Desv. típica riesgo", n(result.stddev_risk_score)],
    ["Riesgo mín / máx", n(result.min_risk_score) + " / " + n(result.max_risk_score)],
    ["Bins distintos", n(result.unique_bins)],
    ["Países distintos", n(result.unique_countries)]
  ];
  // Usar numFilas explícito (offset) para no confundir con `4 + summary.length + 2` del bloque siguiente,
  // que daría última fila 20 → 17 filas de rango frente a 14 filas de datos.
  sheet.getRange(4, 1).offset(0, 0, summary.length, 2).setValues(summary);
  sheet.getRange(4, 1).offset(0, 0, summary.length, 1).setFontWeight("bold");
  sheet.autoResizeColumns(1, 2);

  let row = 4 + summary.length + 2;
  sheet.getRange(row, 1).setValue("Clasificación de tráfico (solo intentos con tier)");
  sheet.getRange(row, 1).setFontWeight("bold");
  row += 2;

  const tierHeader = [["Categoría", "Intentos", "Tier", "% sobre clasificados"]];
  const tierBody = payload.traffic.map((t) => [t.label, t.count, t.tier, t.pct]);
  sheet.getRange(row, 1).offset(0, 0, 1, 4).setValues(tierHeader);
  sheet.getRange(row, 1).offset(0, 0, 1, 4).setFontWeight("bold").setBackground("#f3f3f3");
  row += 1;
  sheet.getRange(row, 1).offset(0, 0, tierBody.length, 4).setValues(tierBody);
  sheet.getRange(row, 2).offset(0, 0, tierBody.length, 1).setNumberFormat("#,##0");
  sheet.getRange(row, 4).offset(0, 0, tierBody.length, 1).setNumberFormat("0.0");

  const chartDataRange = sheet.getRange(row - 1, 1).offset(0, 0, 1 + tierBody.length, 2);
  const chart = sheet
    .newChart()
    .setChartType(Charts.ChartType.COLUMN)
    .addRange(chartDataRange)
    .setNumHeaders(1)
    .setOption("title", "Intentos por categoría de riesgo")
    .setOption("legend", { position: "none" })
    .setOption("colors", payload.traffic.map((t) => t.color))
    .setPosition(row + tierBody.length + 1, 1, 0, 0)
    .build();
  sheet.insertChart(chart);

  row += tierBody.length + 14;
  const gateways = parseJsonArraySafe_(result.gateway_json);
  if (gateways.length) {
    sheet.getRange(row, 1).setValue("Pasarela — intentos y tasa OK");
    sheet.getRange(row, 1).setFontWeight("bold");
    row += 2;
    const gHeader = [["Gateway", "Intentos", "OK", "KO", "% éxito", "Riesgo medio", "Críticos", "Alto+crítico"]];
    const gRows = gateways.map((g) => [
      String(g.gateway || ""),
      n(g.attempts),
      n(g.ok_count),
      n(g.ko_count),
      n(g.success_rate),
      n(g.avg_risk),
      n(g.critical_count),
      n(g.high_risk_count)
    ]);
    sheet.getRange(row, 1).offset(0, 0, 1, gHeader[0].length).setValues(gHeader).setFontWeight("bold").setBackground("#f3f3f3");
    row += 1;
    sheet.getRange(row, 1).offset(0, 0, gRows.length, gHeader[0].length).setValues(gRows);
    sheet.getRange(row, 2).offset(0, 0, gRows.length, 3).setNumberFormat("#,##0");
    sheet.getRange(row, 5).offset(0, 0, gRows.length, 1).setNumberFormat("0.0");
    sheet.getRange(row, 6).offset(0, 0, gRows.length, 1).setNumberFormat("0.0");
    sheet.getRange(row, 7).offset(0, 0, gRows.length, 2).setNumberFormat("#,##0");
    row += gRows.length + 2;
  }

  const countries = parseJsonArraySafe_(result.country_risk_json).slice(0, 15);
  if (countries.length) {
    sheet.getRange(row, 1).setValue("País — resumen (top 15 por riesgo medio)");
    sheet.getRange(row, 1).setFontWeight("bold");
    row += 2;
    const cHeader = [["País", "Intentos", "Alto riesgo", "Riesgo medio", "% alto riesgo"]];
    const cRows = countries.map((c) => [
      String(c.pais_cliente || ""),
      n(c.total_attempts),
      n(c.total_high_risk),
      n(c.avg_risk),
      n(c.high_risk_pct)
    ]);
    sheet.getRange(row, 1).offset(0, 0, 1, cHeader[0].length).setValues(cHeader).setFontWeight("bold").setBackground("#f3f3f3");
    row += 1;
    sheet.getRange(row, 1).offset(0, 0, cRows.length, cHeader[0].length).setValues(cRows);
    sheet.getRange(row, 2).offset(0, 0, cRows.length, 2).setNumberFormat("#,##0");
    sheet.getRange(row, 4).offset(0, 0, cRows.length, 1).setNumberFormat("0.0");
    sheet.getRange(row, 5).offset(0, 0, cRows.length, 1).setNumberFormat("0.0");
  }

  sheet.setFrozenRows(1);
}

/**
 * Test function that simulates BigQuery results to test JSON parsing logic
 * This allows testing the parsing logic without actually querying BigQuery
 */
function testJSONParsingWithMockData() {
  Logger.log('=== Testing JSON Parsing with Mock BigQuery Data ===\n');

  const parseJSONSafe_ = (value, label) => {
    if (value === null || value === undefined || value === "") {
      Logger.log(`[DEBUG] ${label}: value is null/undefined/empty`);
      return null;
    }
    if (typeof value === "object") {
      Logger.log(`[DEBUG] ${label}: value is already an object, type: ${Array.isArray(value) ? 'array' : 'object'}`);
      return value;
    }
    if (typeof value !== "string") {
      Logger.log(`[DEBUG] ${label}: value is not a string, type: ${typeof value}`);
      return null;
    }

    let s = value.trim();

    if (s === "null" || s === "NULL" || s === '""' || s === "''") {
      Logger.log(`[DEBUG] ${label}: value is string "null" or empty quotes`);
      return null;
    }
    if (!s) {
      Logger.log(`[DEBUG] ${label}: value is empty after trim`);
      return null;
    }

    let attempts = 0;
    let currentValue = s;

    while (attempts < 3) {
      try {
        const parsed = JSON.parse(currentValue);

        if (typeof parsed === "string" && attempts < 2) {
          Logger.log(`[DEBUG] ${label}: Parsed to string (likely double-encoded), attempting to parse again (attempt ${attempts + 1})`);
          currentValue = parsed;
          attempts++;
          continue;
        }

        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          if (parsed.arr && Array.isArray(parsed.arr)) {
            Logger.log(`[DEBUG] ${label}: Extracted array from object wrapper`);
            return parsed.arr;
          }
          const keys = Object.keys(parsed).filter(k => !isNaN(Number(k))).map(Number).sort((a, b) => a - b);
          if (keys.length > 0) {
            Logger.log(`[DEBUG] ${label}: Converting object with numeric keys to array`);
            return keys.map(k => parsed[k]);
          }
        }

        Logger.log(`[DEBUG] ${label}: Successfully parsed (attempt ${attempts + 1}), type: ${Array.isArray(parsed) ? 'array' : typeof parsed}, length: ${Array.isArray(parsed) ? parsed.length : 'N/A'}`);
        return parsed;
      } catch (e) {
        if (attempts === 0) {
          const arrayStart = currentValue.indexOf('[');
          const arrayEnd = currentValue.lastIndexOf(']');
          if (arrayStart >= 0 && arrayEnd > arrayStart) {
            try {
              const extracted = currentValue.substring(arrayStart, arrayEnd + 1);
              Logger.log(`[DEBUG] ${label}: Attempting to extract JSON array from string (chars ${arrayStart}-${arrayEnd})`);
              const parsed = JSON.parse(extracted);
              Logger.log(`[DEBUG] ${label}: Successfully extracted and parsed`);
              return parsed;
            } catch (e2) {
              Logger.log(`[WARN] ${label}: Extraction also failed: ${e2.message}`);
            }
          }
        }

        if (attempts > 0) {
          Logger.log(`[WARN] ${label}: Parse failed after ${attempts + 1} attempts: ${e.message}`);
          return null;
        }

        if (currentValue.startsWith('"') && currentValue.endsWith('"') && currentValue.length > 2) {
          try {
            currentValue = JSON.parse(currentValue);
            attempts++;
            continue;
          } catch (e3) {
            Logger.log(`[WARN] ${label}: Unquoting failed: ${e3.message}`);
          }
        }

        Logger.log(`[WARN] JSON parse failed for ${label}: ${e.message}`);
        return null;
      }
    }

    return null;
  };

  const ensureArray_ = (x) => Array.isArray(x) ? x : [];

  const mockTop10Data = [
    { risk_score: 850, risk_tier: 'CRITICAL', recommended_action: 'CHALLENGE', pnr: 'PNR001', gateway: 'gateway1', result: 'KO', amount: 100.50, pais_cliente: 'ES' },
    { risk_score: 750, risk_tier: 'CRITICAL', recommended_action: 'CHALLENGE', pnr: 'PNR002', gateway: 'gateway2', result: 'KO', amount: 200.00, pais_cliente: 'AR' },
    { risk_score: 650, risk_tier: 'HIGH', recommended_action: 'REVIEW', pnr: 'PNR003', gateway: 'gateway1', result: 'OK', amount: 150.75, pais_cliente: 'MX' }
  ];

  Logger.log('\n--- Test 1: Normal JSON string ---');
  const normalJson = JSON.stringify(mockTop10Data);
  Logger.log(`Input: ${normalJson.substring(0, 100)}...`);
  const parsed1 = ensureArray_(parseJSONSafe_(normalJson, "top10_json"));
  Logger.log(`Result: ${parsed1.length} items parsed`);
  if (parsed1.length !== 3) {
    Logger.log(`❌ FAILED: Expected 3 items, got ${parsed1.length}`);
  } else {
    Logger.log(`✅ PASSED: Correctly parsed ${parsed1.length} items`);
  }

  Logger.log('\n--- Test 2: Double-encoded JSON string ---');
  const doubleEncoded = JSON.stringify(normalJson);
  Logger.log(`Input: ${doubleEncoded.substring(0, 100)}...`);
  const parsed2 = ensureArray_(parseJSONSafe_(doubleEncoded, "top10_json"));
  Logger.log(`Result: ${parsed2.length} items parsed`);
  if (parsed2.length !== 3) {
    Logger.log(`❌ FAILED: Expected 3 items, got ${parsed2.length}`);
  } else {
    Logger.log(`✅ PASSED: Correctly parsed ${parsed2.length} items after unquoting`);
  }

  Logger.log('\n--- Test 3: Triple-encoded JSON string ---');
  const tripleEncoded = JSON.stringify(doubleEncoded);
  Logger.log(`Input: ${tripleEncoded.substring(0, 100)}...`);
  const parsed3 = ensureArray_(parseJSONSafe_(tripleEncoded, "top10_json"));
  Logger.log(`Result: ${parsed3.length} items parsed`);
  if (parsed3.length !== 3) {
    Logger.log(`❌ FAILED: Expected 3 items, got ${parsed3.length}`);
  } else {
    Logger.log(`✅ PASSED: Correctly parsed ${parsed3.length} items after multiple unquoting`);
  }

  Logger.log('\n--- Test 4: Empty array ---');
  const emptyArray = '[]';
  const parsed4 = ensureArray_(parseJSONSafe_(emptyArray, "top10_json"));
  Logger.log(`Result: ${parsed4.length} items parsed`);
  if (parsed4.length !== 0) {
    Logger.log(`❌ FAILED: Expected 0 items, got ${parsed4.length}`);
  } else {
    Logger.log(`✅ PASSED: Correctly handled empty array`);
  }

  Logger.log('\n--- Test 5: Null value ---');
  const parsed5 = ensureArray_(parseJSONSafe_(null, "top10_json"));
  Logger.log(`Result: ${parsed5.length} items parsed`);
  if (parsed5.length !== 0) {
    Logger.log(`❌ FAILED: Expected 0 items, got ${parsed5.length}`);
  } else {
    Logger.log(`✅ PASSED: Correctly handled null value`);
  }

  Logger.log('\n--- Test 6: String "null" ---');
  const parsed6 = ensureArray_(parseJSONSafe_("null", "top10_json"));
  Logger.log(`Result: ${parsed6.length} items parsed`);
  if (parsed6.length !== 0) {
    Logger.log(`❌ FAILED: Expected 0 items, got ${parsed6.length}`);
  } else {
    Logger.log(`✅ PASSED: Correctly handled string "null"`);
  }

  Logger.log('\n--- Test 7: Full processing simulation ---');
  const mockResult = {
    total_attempts: 323,
    top10_len: 3,
    top10_json: normalJson
  };

  const top10Len = Number(mockResult.top10_len || 0);
  const topRaw = mockResult.top10_json;
  const topParsed = ensureArray_(parseJSONSafe_(topRaw, "top10_json"));
  const top = topParsed;

  Logger.log(`top10_len: ${top10Len}`);
  Logger.log(`top.length: ${top.length}`);

  if (top10Len > 0 && top.length === 0) {
    Logger.log(`❌ FAILED: top10_len=${top10Len} but parsed top is empty!`);
  } else if (top10Len === top.length && top.length > 0) {
    Logger.log(`✅ PASSED: Successfully parsed ${top.length} top attempts`);
    Logger.log(`First item: ${JSON.stringify(top[0])}`);
  } else {
    Logger.log(`⚠️ WARNING: Length mismatch - top10_len=${top10Len}, parsed=${top.length}`);
  }

  Logger.log('\n=== JSON Parsing Tests Complete ===');
}

/* =========================
   RISK SCORE (codigo_ejemplo.gs) + agregados fuera de BigQuery
========================= */

/**
 * Calcula la puntuación de riesgo y los motivos (misma lógica que codigo_ejemplo.gs).
 * @param {Object} data
 * @returns {{totalScore: number, reasonsText: string}}
 */
function calculateRiskScore(data) {
  let score = 0;
  const reasons = [];
  const rf = Number(data.ratio_fraude) || 0;
  const bw = Number(data.booking_window) != null && !isNaN(Number(data.booking_window)) ? Number(data.booking_window) : null;
  const penalty = Number(data.penalty) != null && !isNaN(Number(data.penalty)) ? Number(data.penalty) : null;

  if (data.ip_country && data.client_country && data.activity_country &&
      data.activity_country !== "_G" &&
      data.ip_country !== data.client_country &&
      data.ip_country !== data.activity_country) {
    score += 2;
    reasons.push("IP diferente del país cliente y país de destino");
  }

  const emailStr = (data.email || "").toString();
  const emailUser = emailStr.split("@")[0].toLowerCase();
  const nameParts = ((data.nombre || "") + " " + (data.apellidos || "")).toLowerCase().split(/\s+/);
  const hasNameMatch = nameParts.some((part) => part.length > 2 && emailUser.indexOf(part) !== -1);
  if (!hasNameMatch && emailUser) {
    score += 1;
    reasons.push("Email no coincide con nombre/apellidos titular");
  }

  const specialChars = emailUser.replace(/[a-z]/g, "").length;
  if (specialChars >= 3) {
    score += 1.5;
    reasons.push("Email con muchos carácteres especiales/números.");
  }

  if (data.canal === "Agencia" || data.canal === "Tienda") {
    score -= 5;
    reasons.push("Canal Agencia/Tienda");
  }

  if (data.reserva_previa_opinada) {
    score -= 5;
    reasons.push("Cliente con reserva previa");
  }

  let attempts = Number(data.attempts) || 0;
  if (attempts === 3) {
    score += 1;
    reasons.push("3 intentos de pago.");
  }
  attempts = Number(data.attempts) || 0;
  if (attempts > 3) {
    score += 2;
    reasons.push("Más de 3 intentos de pago");
  }

  if (rf >= 800 && rf < 999 && bw !== null && bw <= 3 && penalty === 100) {
    score += 4;
    reasons.push("fraud_score >= 800 and ratio_fraude < 999, BW <= 3 no reembolsable");
  }
  if (rf >= 800 && rf < 999 && bw !== null && bw <= 3 && penalty === 0) {
    score += 1;
    reasons.push("fraud_score >= 800 and ratio_fraude < 999, BW <= 3 reembolsable");
  }
  if (rf >= 800 && rf < 999 && bw !== null && bw > 3 && penalty === 100) {
    score += 3;
    reasons.push("fraud_score >= 800 and ratio_fraude < 999, BW > 3 no reembolsable");
  }
  if (rf >= 800 && rf < 999 && bw !== null && bw > 3 && penalty === 0) {
    score += 1;
    reasons.push("fraud_score >= 800, BW > 3 reembolsable");
  }
  if (rf >= 500 && rf <= 799 && bw !== null && bw <= 3 && penalty === 100) {
    score += 4;
    reasons.push("fraud_score between 500.0 and 799.0, BW <= 3 no reembolsable");
  }
  if (rf >= 500 && rf <= 799 && bw !== null && bw <= 3 && penalty === 0) {
    score += 1;
    reasons.push("fraud_score between 500.0 and 799.0, BW <= 3 reembolsable");
  }
  if (rf >= 500 && rf <= 799 && bw !== null && bw > 3 && penalty === 100) {
    score += 3;
    reasons.push("fraud_score between 500.0 and 799.0, BW > 3 no reembolsable");
  }
  if (rf >= 500 && rf <= 799 && bw !== null && bw > 3 && penalty === 0 && data.is_first_booking === true) {
    score += 1;
    reasons.push("fraud_score between 500.0 and 799.0, BW > 3 reembolsable");
  }
  if (rf >= 100 && rf <= 499 && bw !== null && bw <= 3 && penalty === 100 && data.is_first_booking === true) {
    score += 2;
    reasons.push("fraud_score between 100.0 and 499.0, BW <= 3 no reembolsable");
  }
  if (rf >= 100 && rf <= 499 && bw !== null && bw <= 3 && penalty === 0) {
    score += 0.5;
    reasons.push("fraud_score between 100.0 and 499.0, BW <= 3 reembolsable");
  }
  if (rf >= 100 && rf <= 499 && bw !== null && bw > 3 && penalty === 100 && data.is_first_booking === true) {
    score += 1;
    reasons.push("fraud_score between 100.0 and 499.0, BW > 3 no reembolsable, primera reserva");
  }
  if (rf >= 999 && bw !== null && bw <= 2 && penalty === 100) {
    score += 6;
    reasons.push("fraud_score = 999, <= 2 no reembolsable");
  }
  if (rf >= 999 && bw !== null && bw <= 2 && penalty === 0) {
    score += 3;
    reasons.push("fraud_score = 999, <= 2 reembolsable");
  }
  if (rf >= 999 && bw !== null && bw >= 3 && penalty === 100) {
    score += 5;
    reasons.push("fraud_score = 999, >= 3 no reembolsable");
  }
  if (rf >= 999 && bw !== null && bw >= 3 && penalty === 0) {
    score += 2;
    reasons.push("fraud_score = 999, >= 3 reembolsable");
  }

  return { totalScore: score, reasonsText: reasons.join(" | ") };
}

function classifyScoreEjemplo_(score) {
  if (score < 1) return "muy buena";
  if (score >= 1 && score < 3) return "buena";
  if (score >= 3 && score <= 5) return "mala";
  if (score > 5) return "muy mala";
  return "buena";
}

function mapEjemploCategoryToRiskTier_(cat) {
  const m = { "muy buena": "LOW", buena: "MEDIUM", mala: "HIGH", "muy mala": "CRITICAL" };
  return m[cat] || "MEDIUM";
}

function recommendedActionForTier_(tier) {
  if (tier === "CRITICAL") return "CHALLENGE";
  if (tier === "HIGH") return "REVIEW";
  if (tier === "MEDIUM") return "MONITOR";
  return "ALLOW";
}

function applyEjemploRiskToAttemptRow_(row) {
  const { totalScore, reasonsText } = calculateRiskScore(rowToRiskScoreInput_(row));
  const cat = classifyScoreEjemplo_(totalScore);
  const risk_tier = mapEjemploCategoryToRiskTier_(cat);
  const risk_score = Math.round(totalScore * 100);
  return Object.assign({}, row, {
    risk_score,
    risk_tier,
    recommended_action: recommendedActionForTier_(risk_tier),
    motivos_puntuacion: reasonsText || "",
    total_score_ejemplo: totalScore
  });
}

function rowToRiskScoreInput_(row) {
  const em = row.email === "N/A" || row.email == null ? "" : String(row.email);
  return {
    ip_country: String(row.geo_ip || "").trim(),
    client_country: String(row.pais_cliente || "").trim(),
    activity_country: String(row.activity_country_abreviado || "").trim(),
    email: em,
    nombre: row.nombre || "",
    apellidos: row.apellidos || "",
    canal: row.canal || "",
    reserva_previa_opinada: row.reserva_previa_opinada_flag === true || row.reserva_previa_opinada_flag === "true",
    attempts: row.total_intentos_pago != null ? Number(row.total_intentos_pago) : 0,
    ratio_fraude: row.ratio_fraude,
    booking_window: row.booking_window_days != null && row.booking_window_days !== "" ? Number(row.booking_window_days) : null,
    penalty: row.penalty != null && row.penalty !== "" ? Number(row.penalty) : null,
    is_first_booking: row.is_first_booking === true || row.is_first_booking === "true"
  };
}

function parseAttemptsJson_(result) {
  const raw = result.attempts_json;
  if (raw == null || raw === "") return [];
  if (Array.isArray(raw)) return raw;
  if (typeof raw === "string") {
    try {
      const p = JSON.parse(raw);
      return Array.isArray(p) ? p : [];
    } catch (e) {
      return [];
    }
  }
  return [];
}

function stddevSampleOneDecimal_(values) {
  const n = values.length;
  if (n < 2) return 0;
  const mean = values.reduce((a, b) => a + b, 0) / n;
  const v = values.reduce((s, x) => s + (x - mean) * (x - mean), 0) / (n - 1);
  return Math.round(Math.sqrt(v) * 10) / 10;
}

function medianSorted_(sorted) {
  if (!sorted.length) return 0;
  const m = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[m] : (sorted[m - 1] + sorted[m]) / 2;
}

function quantileSorted_(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor(p * (sorted.length - 1))));
  return sorted[idx];
}

/**
 * A partir de attempts_json (filas en ventana), aplica calculateRiskScore y reconstruye
 * todas las métricas que antes calculaba BigQuery sobre `labeled`.
 * @param {Object} result — mutado in-place
 */
function recomputeRiskMetricsInResult_(result) {
  const rawRows = parseAttemptsJson_(result);
  const labeled = rawRows.map((row) => applyEjemploRiskToAttemptRow_(row));

  const n = (v) => (v === null || v === undefined || v === "" ? 0 : Number(v));
  const scores = labeled.map((r) => n(r.risk_score));

  result.critical_count = labeled.filter((r) => r.risk_tier === "CRITICAL").length;
  result.high_count = labeled.filter((r) => r.risk_tier === "HIGH").length;
  result.medium_count = labeled.filter((r) => r.risk_tier === "MEDIUM").length;
  result.low_count = labeled.filter((r) => r.risk_tier === "LOW").length;

  result.avg_risk_score = scores.length ? Math.round((scores.reduce((a, b) => a + b, 0) / scores.length) * 10) / 10 : 0;
  result.stddev_risk_score = stddevSampleOneDecimal_(scores);
  result.min_risk_score = scores.length ? Math.min.apply(null, scores) : 0;
  result.max_risk_score = scores.length ? Math.max.apply(null, scores) : 0;

  const bins = new Set();
  const countries = new Set();
  for (let i = 0; i < labeled.length; i++) {
    const b = labeled[i].cardbin;
    if (b != null && String(b).trim() !== "") bins.add(String(b));
    const c = labeled[i].pais_cliente;
    if (c != null && String(c).trim() !== "") countries.add(String(c));
  }
  result.unique_bins = bins.size;
  result.unique_countries = countries.size;

  const totalTier = result.critical_count + result.high_count + result.medium_count + result.low_count;
  const shareArr = ["CRITICAL", "HIGH", "MEDIUM", "LOW"].map((tier) => {
    const cnt = labeled.filter((r) => r.risk_tier === tier).length;
    const pct = totalTier > 0 ? Math.round((1000 * cnt) / totalTier) / 10 : 0;
    return { risk_tier: tier, cnt, pct_total: pct };
  });
  result.share_json = JSON.stringify(shareArr);

  const tierOrder = { CRITICAL: 1, HIGH: 2, MEDIUM: 3, LOW: 4 };
  const tierKeys = Object.keys(tierOrder);
  const tierResultBase = tierKeys.map((tier) => {
    const rows = labeled.filter((r) => r.risk_tier === tier);
    const attempts = rows.length;
    const ok_attempts = rows.filter((r) => String(r.result || "").toLowerCase() === "ok").length;
    const ko_attempts = rows.filter((r) => String(r.result || "").toLowerCase() === "ko").length;
    return { risk_tier: tier, attempts, ok_attempts, ko_attempts };
  }).filter((x) => x.attempts > 0);
  const totalAttemptsTier = tierResultBase.reduce((s, x) => s + x.attempts, 0);
  const tierResultTot = tierResultBase.map((x) =>
    Object.assign({}, x, {
      total_attempts: totalAttemptsTier,
      pct_total: totalAttemptsTier > 0 ? Math.round((1000 * x.attempts) / totalAttemptsTier) / 10 : 0,
      ok_pct_within_tier: x.attempts > 0 ? Math.round((1000 * x.ok_attempts) / x.attempts) / 10 : 0,
      ko_pct_within_tier: x.attempts > 0 ? Math.round((1000 * x.ko_attempts) / x.attempts) / 10 : 0
    })
  );
  tierResultTot.sort((a, b) => tierOrder[a.risk_tier] - tierOrder[b.risk_tier]);
  result.tier_result_json = JSON.stringify(tierResultTot);

  const topSorted = labeled.slice().sort((a, b) => n(b.risk_score) - n(a.risk_score));
  const topN = topSorted.slice(0, CFG.TOP_N);
  result.top10_arr = topN.map((r) => ({
    risk_score: r.risk_score,
    risk_tier: r.risk_tier,
    recommended_action: r.recommended_action,
    pnr: r.pnr,
    email: r.email,
    attempt_dt: r.attempt_dt,
    gateway: r.gateway,
    result: r.result,
    amount: r.amount,
    currency: r.currency,
    card_country: r.card_country,
    issuercountry: r.issuercountry,
    countrycode: r.countrycode,
    cardbin: r.cardbin,
    pais_cliente: r.pais_cliente,
    id_producto: r.id_producto,
    product_type: r.product_type,
    ratio_fraude: r.ratio_fraude,
    booking_window_days: r.booking_window_days,
    score_booking_window: r.score_booking_window,
    score_catalog_risk: r.score_catalog_risk,
    score_country_risk: r.score_country_risk,
    motivos_puntuacion: r.motivos_puntuacion
  }));
  result.top10_len = result.top10_arr.length;

  const binMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const bin = labeled[i].cardbin;
    if (bin == null || String(bin).trim() === "") continue;
    const k = String(bin);
    if (!binMap[k]) binMap[k] = { cardbin: k, scores: [], amounts: [] };
    binMap[k].scores.push(n(labeled[i].risk_score));
    binMap[k].amounts.push(n(labeled[i].amount));
  }
  const binRows = Object.keys(binMap)
    .map((k) => {
      const o = binMap[k];
      const attempts = o.scores.length;
      if (attempts < 2) return null;
      const avg_score = Math.round((o.scores.reduce((a, b) => a + b, 0) / attempts) * 10) / 10;
      const max_score = Math.max.apply(null, o.scores);
      const total_amount = o.amounts.reduce((a, b) => a + b, 0);
      return { cardbin: k, attempts, avg_score, max_score, total_amount };
    })
    .filter(Boolean)
    .sort((a, b) => b.avg_score - a.avg_score)
    .slice(0, 5);
  result.bin_risk_json = JSON.stringify(binRows);

  const pnrMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const p = labeled[i].pnr;
    if (p == null || p === "") continue;
    const k = String(p);
    if (!pnrMap[k]) pnrMap[k] = [];
    pnrMap[k].push(labeled[i]);
  }
  const pnrRetries = Object.keys(pnrMap)
    .map((k) => {
      const rows = pnrMap[k];
      if (rows.length < 3) return null;
      const results = [...new Set(rows.map((r) => r.result).filter((x) => x != null))].slice(0, 5);
      const sc = rows.map((r) => n(r.risk_score));
      return {
        pnr: k,
        retry_count: rows.length,
        unique_results: results.length,
        avg_risk: Math.round((sc.reduce((a, b) => a + b, 0) / sc.length) * 10) / 10,
        max_risk: Math.max.apply(null, sc),
        results
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.retry_count - a.retry_count)
    .slice(0, 5);
  result.pnr_retries_json = JSON.stringify(pnrRetries);

  const emailMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const em = labeled[i].email;
    if (!em || em === "N/A") continue;
    if (!emailMap[em]) emailMap[em] = [];
    emailMap[em].push(labeled[i]);
  }
  const velocityRows = Object.keys(emailMap)
    .map((em) => {
      const rows = emailMap[em];
      if (rows.length < 2) return null;
      const pnrs = new Set(rows.map((r) => r.pnr).filter(Boolean));
      const times = rows
        .map((r) => (r.attempt_ts_iso ? new Date(r.attempt_ts_iso).getTime() : NaN))
        .filter((t) => !isNaN(t));
      const spanMin =
        times.length >= 2 ? Math.round((Math.max.apply(null, times) - Math.min.apply(null, times)) / 60000) : 0;
      let velocity_risk_score = 0;
      if (rows.length >= 5) velocity_risk_score = 50;
      else if (rows.length >= 3) velocity_risk_score = 30;
      return {
        email: em,
        attempts_count: rows.length,
        unique_bookings: pnrs.size,
        time_span_minutes: spanMin,
        velocity_risk_score
      };
    })
    .filter(Boolean)
    .filter((x) => x.attempts_count >= 3)
    .sort((a, b) => b.attempts_count - a.attempts_count)
    .slice(0, 10);
  result.velocity_json = JSON.stringify(velocityRows);

  const timeMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const h = labeled[i].hour_of_day;
    const d = labeled[i].day_of_week;
    if (h == null || d == null) continue;
    const key = String(h) + "_" + String(d);
    if (!timeMap[key]) timeMap[key] = { hour_of_day: n(h), day_of_week: n(d), rows: [] };
    timeMap[key].rows.push(labeled[i]);
  }
  const hourlyAgg = {};
  Object.keys(timeMap).forEach((key) => {
    const { hour_of_day, rows } = timeMap[key];
    if (!hourlyAgg[hour_of_day]) hourlyAgg[hour_of_day] = { hour_of_day, attempts: 0, high_risk_count: 0, sumAvg: 0, n: 0 };
    const attempts = rows.length;
    const high = rows.filter((r) => r.risk_tier === "CRITICAL" || r.risk_tier === "HIGH").length;
    const avgRow = rows.length ? rows.reduce((s, r) => s + n(r.risk_score), 0) / rows.length : 0;
    hourlyAgg[hour_of_day].attempts += attempts;
    hourlyAgg[hour_of_day].high_risk_count += high;
    hourlyAgg[hour_of_day].sumAvg += avgRow;
    hourlyAgg[hour_of_day].n += 1;
  });
  const hourlyArr = Object.keys(hourlyAgg)
    .map((h) => {
      const o = hourlyAgg[h];
      return {
        hour_of_day: n(h),
        total_attempts: o.attempts,
        total_high_risk: o.high_risk_count,
        avg_risk: o.n ? Math.round((o.sumAvg / o.n) * 10) / 10 : 0
      };
    })
    .sort((a, b) => a.hour_of_day - b.hour_of_day);
  result.hourly_json = JSON.stringify(hourlyArr);

  const amountByTier = {};
  for (let i = 0; i < labeled.length; i++) {
    const t = labeled[i].risk_tier || "LOW";
    const ae = n(labeled[i].amount_eur);
    if (!amountByTier[t]) amountByTier[t] = [];
    amountByTier[t].push(ae);
  }
  const tierKeysAmt = ["CRITICAL", "HIGH", "MEDIUM", "LOW"];
  const amountAnalysis = tierKeysAmt
    .map((tier) => {
      const arr = amountByTier[tier] || [];
      if (!arr.length) return null;
      const attempts = arr.length;
      const sorted = arr.slice().sort((a, b) => a - b);
      const high_amount_count = arr.filter((x) => x >= 1000).length;
      const round_amount_count = arr.filter((x) => x > 0 && x % 100 === 0).length;
      return {
        risk_tier: tier,
        attempts,
        avg_amount: Math.round((arr.reduce((a, b) => a + b, 0) / attempts) * 100) / 100,
        median_amount: Math.round(medianSorted_(sorted) * 100) / 100,
        p95_amount: Math.round(quantileSorted_(sorted, 0.95) * 100) / 100,
        high_amount_count,
        round_amount_count,
        round_amount_pct: attempts > 0 ? Math.round((1000 * round_amount_count) / attempts) / 10 : 0
      };
    })
    .filter(Boolean)
    .sort((a, b) => tierOrder[a.risk_tier] - tierOrder[b.risk_tier]);
  result.amount_analysis_json = JSON.stringify(amountAnalysis);

  const amountAnom = labeled
    .filter((r) => {
      const ae = n(r.amount_eur);
      const hi = ae >= 1000;
      const rnd = ae > 0 && ae % 100 === 0;
      const tier = r.risk_tier;
      return (hi || rnd) && (tier === "CRITICAL" || tier === "HIGH");
    })
    .sort((a, b) => n(b.risk_score) - n(a.risk_score))
    .slice(0, 10)
    .map((r) => {
      const ae = n(r.amount_eur);
      let anomaly_type = "Normal";
      if (ae >= 1000) anomaly_type = "High Amount";
      else if (ae > 0 && ae % 100 === 0) anomaly_type = "Round Number";
      return {
        pnr: r.pnr,
        email: r.email,
        amount: ae,
        risk_score: r.risk_score,
        risk_tier: r.risk_tier,
        anomaly_type
      };
    });
  result.amount_anomalies_json = JSON.stringify(amountAnom);

  const geoMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const pc = labeled[i].pais_cliente;
    if (pc == null || String(pc).trim() === "") continue;
    const cc = labeled[i].card_country;
    const key = String(pc) + "\t" + String(cc != null ? cc : "");
    if (!geoMap[key]) geoMap[key] = { pais_cliente: pc, card_country: cc, rows: [] };
    geoMap[key].rows.push(labeled[i]);
  }
  const geoRows2 = Object.keys(geoMap).map((key) => {
    const g = geoMap[key];
    const attempts = g.rows.length;
    const high = g.rows.filter((r) => r.risk_tier === "CRITICAL" || r.risk_tier === "HIGH").length;
    const avg_risk = attempts ? Math.round((g.rows.reduce((s, r) => s + n(r.risk_score), 0) / attempts) * 10) / 10 : 0;
    const high_risk_pct = attempts ? Math.round((1000 * high) / attempts) / 10 : 0;
    return { pais_cliente: g.pais_cliente, attempts, high_risk_count: high, avg_risk, high_risk_pct };
  });
  const countryRoll = {};
  for (let i = 0; i < geoRows2.length; i++) {
    const g = geoRows2[i];
    const pc = String(g.pais_cliente);
    if (!countryRoll[pc]) countryRoll[pc] = { total_attempts: 0, total_high_risk: 0, avgs: [] };
    countryRoll[pc].total_attempts += g.attempts;
    countryRoll[pc].total_high_risk += g.high_risk_count;
    countryRoll[pc].avgs.push(g.avg_risk);
  }
  const countryRisk = Object.keys(countryRoll)
    .map((pc) => {
      const o = countryRoll[pc];
      if (o.total_attempts < 5) return null;
      const avg_risk = o.avgs.length ? Math.round((o.avgs.reduce((a, b) => a + b, 0) / o.avgs.length) * 10) / 10 : 0;
      const high_risk_pct = Math.round((1000 * o.total_high_risk) / o.total_attempts) / 10;
      return {
        pais_cliente: pc,
        total_attempts: o.total_attempts,
        total_high_risk: o.total_high_risk,
        avg_risk,
        high_risk_pct
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.avg_risk - a.avg_risk)
    .slice(0, 10);
  result.country_risk_json = JSON.stringify(countryRisk);

  const gwMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const gw = labeled[i].gateway;
    if (gw == null || String(gw).trim() === "") continue;
    const k = String(gw);
    if (!gwMap[k]) gwMap[k] = [];
    gwMap[k].push(labeled[i]);
  }
  const gatewayJson = Object.keys(gwMap)
    .map((k) => {
      const rows = gwMap[k];
      if (rows.length < 2) return null;
      const ok_count = rows.filter((r) => String(r.result || "").toLowerCase() === "ok").length;
      const ko_count = rows.filter((r) => String(r.result || "").toLowerCase() === "ko").length;
      const attempts = rows.length;
      const sc = rows.map((r) => n(r.risk_score));
      return {
        gateway: k,
        attempts,
        ok_count,
        ko_count,
        success_rate: Math.round((1000 * ok_count) / attempts) / 10,
        avg_risk: Math.round((sc.reduce((a, b) => a + b, 0) / attempts) * 10) / 10,
        critical_count: rows.filter((r) => r.risk_tier === "CRITICAL").length,
        high_risk_count: rows.filter((r) => r.risk_tier === "CRITICAL" || r.risk_tier === "HIGH").length
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.avg_risk - a.avg_risk);
  result.gateway_json = JSON.stringify(gatewayJson);

  const prodMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const idp = labeled[i].id_producto;
    const pt = labeled[i].product_type;
    if (idp == null) continue;
    const k = String(idp) + "\t" + String(pt != null ? pt : "");
    if (!prodMap[k]) prodMap[k] = { id_producto: idp, product_type: pt, rows: [] };
    prodMap[k].rows.push(labeled[i]);
  }
  const productVel = Object.keys(prodMap)
    .map((k) => {
      const rows = prodMap[k].rows;
      if (rows.length < 3) return null;
      const purchase_count = rows.length;
      const unique_emails = new Set(rows.map((r) => r.email).filter((e) => e && e !== "N/A")).size;
      const unique_countries = new Set(rows.map((r) => r.pais_cliente).filter(Boolean)).size;
      const unique_bookings = new Set(rows.map((r) => r.pnr).filter(Boolean)).size;
      const times = rows
        .map((r) => (r.attempt_ts_iso ? new Date(r.attempt_ts_iso).getTime() : NaN))
        .filter((t) => !isNaN(t));
      const time_span_minutes =
        times.length >= 2 ? Math.round((Math.max.apply(null, times) - Math.min.apply(null, times)) / 60000) : 0;
      const sc = rows.map((r) => n(r.risk_score));
      const avg_risk = Math.round((sc.reduce((a, b) => a + b, 0) / sc.length) * 10) / 10;
      const high_risk_count = rows.filter((r) => r.risk_tier === "CRITICAL" || r.risk_tier === "HIGH").length;
      return {
        id_producto: prodMap[k].id_producto,
        product_type: prodMap[k].product_type,
        purchase_count,
        unique_emails,
        unique_countries,
        unique_bookings,
        time_span_minutes,
        avg_risk,
        high_risk_count
      };
    })
    .filter(Boolean)
    .filter(
      (x) =>
        x.purchase_count >= 5 ||
        (x.purchase_count >= 3 && x.unique_emails >= 2)
    )
    .sort((a, b) => b.purchase_count - a.purchase_count || b.avg_risk - a.avg_risk)
    .slice(0, 10);
  result.product_velocity_json = JSON.stringify(productVel);

  const cpMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const pc = labeled[i].pais_cliente;
    const idp = labeled[i].id_producto;
    const pt = labeled[i].product_type;
    if (pc == null || idp == null) continue;
    const k = String(pc) + "\t" + String(idp) + "\t" + String(pt != null ? pt : "");
    if (!cpMap[k]) cpMap[k] = { pais_cliente: pc, id_producto: idp, product_type: pt, rows: [] };
    cpMap[k].rows.push(labeled[i]);
  }
  const countryProduct = Object.keys(cpMap)
    .map((k) => {
      const rows = cpMap[k].rows;
      if (rows.length < 2) return null;
      const purchase_count = rows.length;
      const unique_emails = new Set(rows.map((r) => r.email).filter((e) => e && e !== "N/A")).size;
      const unique_bookings = new Set(rows.map((r) => r.pnr).filter(Boolean)).size;
      const times = rows
        .map((r) => (r.attempt_ts_iso ? new Date(r.attempt_ts_iso).getTime() : NaN))
        .filter((t) => !isNaN(t));
      const time_span_minutes =
        times.length >= 2 ? Math.round((Math.max.apply(null, times) - Math.min.apply(null, times)) / 60000) : 0;
      const sc = rows.map((r) => n(r.risk_score));
      const avg_risk = Math.round((sc.reduce((a, b) => a + b, 0) / sc.length) * 10) / 10;
      const high_risk_count = rows.filter((r) => r.risk_tier === "CRITICAL" || r.risk_tier === "HIGH").length;
      const high_risk_pct = Math.round((1000 * high_risk_count) / purchase_count) / 10;
      return {
        pais_cliente: cpMap[k].pais_cliente,
        id_producto: cpMap[k].id_producto,
        product_type: cpMap[k].product_type,
        purchase_count,
        unique_emails,
        unique_bookings,
        time_span_minutes,
        avg_risk,
        high_risk_count,
        high_risk_pct
      };
    })
    .filter(Boolean)
    .filter(
      (x) =>
        x.purchase_count >= 3 ||
        (x.purchase_count >= 2 && x.unique_emails >= 2)
    )
    .sort((a, b) => b.purchase_count - a.purchase_count || b.avg_risk - a.avg_risk)
    .slice(0, 10);
  result.country_product_json = JSON.stringify(countryProduct);

  const epMap = {};
  for (let i = 0; i < labeled.length; i++) {
    const em = labeled[i].email;
    const idp = labeled[i].id_producto;
    const pt = labeled[i].product_type;
    if (!em || em === "N/A" || idp == null) continue;
    const k = em + "\t" + String(idp) + "\t" + String(pt != null ? pt : "");
    if (!epMap[k]) epMap[k] = { email: em, id_producto: idp, product_type: pt, rows: [] };
    epMap[k].rows.push(labeled[i]);
  }
  const emailProduct = Object.keys(epMap)
    .map((k) => {
      const rows = epMap[k].rows;
      if (rows.length < 2) return null;
      const purchase_count = rows.length;
      if (purchase_count < 3) return null;
      const unique_bookings = new Set(rows.map((r) => r.pnr).filter(Boolean)).size;
      const unique_countries = new Set(rows.map((r) => r.pais_cliente).filter(Boolean)).size;
      const times = rows
        .map((r) => (r.attempt_ts_iso ? new Date(r.attempt_ts_iso).getTime() : NaN))
        .filter((t) => !isNaN(t));
      const time_span_minutes =
        times.length >= 2 ? Math.round((Math.max.apply(null, times) - Math.min.apply(null, times)) / 60000) : 0;
      const sc = rows.map((r) => n(r.risk_score));
      const avg_risk = Math.round((sc.reduce((a, b) => a + b, 0) / sc.length) * 10) / 10;
      const max_risk = Math.max.apply(null, sc);
      const high_risk_count = rows.filter((r) => r.risk_tier === "CRITICAL" || r.risk_tier === "HIGH").length;
      return {
        email: epMap[k].email,
        id_producto: epMap[k].id_producto,
        product_type: epMap[k].product_type,
        purchase_count,
        unique_bookings,
        unique_countries,
        time_span_minutes,
        avg_risk,
        max_risk,
        high_risk_count
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.purchase_count - a.purchase_count || b.max_risk - a.max_risk)
    .slice(0, 10);
  result.email_product_json = JSON.stringify(emailProduct);
}

/* =========================
   SQL
========================= */

function buildSQL_() {
  const tz = CFG.TIMEZONE;

  const excludeTypologyClause =
    (CFG.EXCLUDE_TYPOLOGY !== null)
      ? `AND (r.typology IS NULL OR r.typology != ${CFG.EXCLUDE_TYPOLOGY})`
      : "";

  return `
WITH params AS (
  SELECT
    DATETIME_SUB(DATETIME_TRUNC(DATETIME(CURRENT_TIMESTAMP(), '${tz}'), DAY), INTERVAL 1 MICROSECOND) AS run_anchor_dt,
    CAST(NULL AS INT64) AS window_minutes,
    ${CFG.W_BOOKING_WINDOW} AS w_booking_window,
    ${CFG.W_CATALOG_RISK}   AS w_catalog_risk,
    ${CFG.W_COUNTRY_RISK}   AS w_country_risk
),
bounds AS (
  SELECT
    run_anchor_dt,
    DATETIME_SUB(DATETIME_TRUNC(DATETIME(CURRENT_TIMESTAMP(), '${tz}'), DAY), INTERVAL 1 DAY) AS window_start_dt,
    run_anchor_dt AS window_end_dt
  FROM params
),

reservas_por_email AS (
  SELECT
    email,
    MIN(fechaReserva) AS min_fecha_reserva_email,
    COUNTIF(estado = 4) > 0 AS reserva_previa_opinada_flag
  FROM \`${CFG.RESERVAS_TABLE}\`
  GROUP BY email
),

base AS (
  SELECT
    pt.pnr,
    CAST(pt.transaction_id AS STRING) AS transaction_id,
    pt.${CFG.PAYMENT_TS_FIELD} AS attempt_dt,
    TIMESTAMP(pt.${CFG.PAYMENT_TS_FIELD}, '${tz}') AS attempt_ts,
    pt.gateway,
    pt.result,
    pt.response,
    pt.amount,
    pt.currency,
    pt.card_country,
    pt.issuercountry,
    pt.countrycode,
    pt.cardbin,
    COALESCE(pt.geo_ip, '') AS geo_ip,

    r.old_id,
    r.id_producto,
    r.product_type,
    r.pais_cliente,
    r.typology,
    r.canal,
    COALESCE(r.email, 'N/A') AS email,
    r.nombre AS nombre,
    r.apellidos AS apellidos,
    COALESCE(r.precioinicialeuros, pt.amount) AS amount_eur,
    DATE_DIFF(DATE(r.fecha), DATE(r.fechaReserva), DAY) AS booking_window_days,

    CAST(c.ratio_fraude AS INT64) AS ratio_fraude,
    pais.iso2 AS activity_country_abreviado,
    rpe.reserva_previa_opinada_flag,
    (r.fechaReserva IS NOT NULL AND rpe.min_fecha_reserva_email IS NOT NULL AND r.fechaReserva = rpe.min_fecha_reserva_email) AS is_first_booking,
    SAFE_CAST(acp.penalty AS INT64) AS penalty,
    COUNT(DISTINCT pt.transaction_id) OVER (PARTITION BY r.old_id, r.product_type) AS total_intentos_pago,
    EXTRACT(HOUR FROM TIMESTAMP(pt.${CFG.PAYMENT_TS_FIELD}, '${tz}')) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pt.${CFG.PAYMENT_TS_FIELD}, '${tz}')) AS day_of_week
  FROM \`${CFG.PAYMENTS_TABLE}\` pt
  LEFT JOIN \`${CFG.RESERVAS_TABLE}\` r
    ON r.cart_id = pt.pnr
  LEFT JOIN reservas_por_email rpe ON r.email = rpe.email
  LEFT JOIN \`${CFG.CATALOGO_TABLE}\` c
    ON r.id_producto = c.id_producto
   AND r.product_type = c.product_type
  LEFT JOIN \`${CFG.DESTINOS_TABLE}\` AS d ON c.id_ciudad = d.id
  LEFT JOIN \`${CFG.PAISES_TABLE}\` AS pais ON d.id_pais = pais.id
  LEFT JOIN \`${CFG.ACP_TABLE}\` AS acp ON c.id_producto = CAST(acp.activity_id AS STRING)
  CROSS JOIN bounds b
  WHERE pt.${CFG.PAYMENT_TS_FIELD} >= b.window_start_dt
    AND pt.${CFG.PAYMENT_TS_FIELD} <= b.window_end_dt
    AND pt.payment_type = '${CFG.PAYMENT_TYPE_FILTER}'
    ${excludeTypologyClause}
),

enriched AS (
  SELECT
    b.*,
    CASE
      WHEN booking_window_days IS NULL THEN 40
      WHEN booking_window_days <= 1 THEN 90
      WHEN booking_window_days <= 3 THEN 70
      WHEN booking_window_days <= 9 THEN 30
      ELSE 0
    END AS score_booking_window,

    CASE
      WHEN ratio_fraude IS NULL THEN 30
      WHEN ratio_fraude >= 999 THEN 90
      WHEN ratio_fraude >= 900 THEN 80
      WHEN ratio_fraude >= 800 THEN 70
      WHEN ratio_fraude >= 700 THEN 60
      WHEN ratio_fraude >= 600 THEN 50
      WHEN ratio_fraude >= 500 THEN 40
      WHEN ratio_fraude >= 400 THEN 20
      WHEN ratio_fraude >= 100 THEN 10
      ELSE 0
    END AS score_catalog_risk,

    CASE
      WHEN pais_cliente IS NULL OR TRIM(pais_cliente) = '' THEN 40
      WHEN UPPER(pais_cliente) IN ('ES','IT','FR') THEN 0
      WHEN UPPER(pais_cliente) IN ('AR','MX','CO','PE','UY','CR','BR') THEN 30
      ELSE 70
    END AS score_country_risk
  FROM base b
),

traffic AS (
  SELECT
    COUNT(*) AS total_attempts,
    COUNTIF(LOWER(result) = 'ok') AS ok_attempts,
    COUNTIF(LOWER(result) = 'ko') AS ko_attempts,
    COUNTIF(result IS NULL OR LOWER(result) NOT IN ('ok','ko')) AS other_attempts,
    COUNT(DISTINCT pnr) AS unique_pnrs
  FROM enriched
),

attempts_serialized AS (
  SELECT COALESCE(
    TO_JSON_STRING(
      ARRAY_AGG(
        STRUCT(
          pnr,
          transaction_id,
          email,
          CAST(attempt_dt AS STRING) AS attempt_dt,
          FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', attempt_ts) AS attempt_ts_iso,
          hour_of_day,
          day_of_week,
          gateway,
          result,
          CAST(amount AS FLOAT64) AS amount,
          currency,
          card_country,
          issuercountry,
          countrycode,
          cardbin,
          pais_cliente,
          id_producto,
          product_type,
          canal,
          CAST(ratio_fraude AS INT64) AS ratio_fraude,
          CAST(booking_window_days AS INT64) AS booking_window_days,
          CAST(score_booking_window AS INT64) AS score_booking_window,
          CAST(score_catalog_risk AS INT64) AS score_catalog_risk,
          CAST(score_country_risk AS INT64) AS score_country_risk,
          CAST(amount_eur AS FLOAT64) AS amount_eur,
          geo_ip,
          nombre,
          apellidos,
          activity_country_abreviado,
          reserva_previa_opinada_flag,
          is_first_booking,
          penalty,
          CAST(total_intentos_pago AS INT64) AS total_intentos_pago
        )
        ORDER BY attempt_ts DESC
      )
    ),
    '[]'
  ) AS attempts_json
  FROM enriched
)

SELECT
  (SELECT run_anchor_dt FROM bounds) AS run_anchor_dt,
  (SELECT window_start_dt FROM bounds) AS window_start_dt,
  (SELECT window_end_dt FROM bounds) AS window_end_dt,
  (SELECT window_minutes FROM params) AS window_minutes,
  ${CFG.EXCLUDE_TYPOLOGY !== null ? CFG.EXCLUDE_TYPOLOGY : "NULL"} AS exclude_typology,

  (SELECT total_attempts FROM traffic) AS total_attempts,
  (SELECT ok_attempts FROM traffic) AS ok_attempts,
  (SELECT ko_attempts FROM traffic) AS ko_attempts,
  (SELECT other_attempts FROM traffic) AS other_attempts,
  (SELECT unique_pnrs FROM traffic) AS unique_pnrs,

  (SELECT attempts_json FROM attempts_serialized) AS attempts_json
`;
}

/* =========================
   BIGQUERY
========================= */

function runBigQuerySingleRow_(projectId, location, sql) {
  const res = BigQuery.Jobs.query({ query: sql, useLegacySql: false, location, timeoutMs: 60000 }, projectId);
  const jobId = res.jobReference.jobId;
  Logger.log(`[INFO] BigQuery job ID: ${jobId}`);

  let poll = BigQuery.Jobs.getQueryResults(projectId, jobId, { location });
  let attempts = 0;
  while (!poll.jobComplete && attempts < 20) {
    Utilities.sleep(500);
    poll = BigQuery.Jobs.getQueryResults(projectId, jobId, { location });
    attempts++;
  }
  if (!poll.jobComplete) throw new Error(`BigQuery job timeout. Job ID: ${jobId}`);
  if (poll.errors && poll.errors.length) throw new Error(`BigQuery errors: ${JSON.stringify(poll.errors)}`);

  const fields = poll.schema.fields;
  const row = (poll.rows && poll.rows[0]) ? poll.rows[0].f : null;
  if (!row) return {};

  const out = {};
  for (let i = 0; i < fields.length; i++) {
    let value = row[i].v;
    const fieldName = fields[i].name;

    if (fieldName === 'top10_arr') {
      if (value && typeof value === 'object') {
        Logger.log(`[DEBUG] top10_arr received as object`);

        try {
          if (Array.isArray(value)) {
            Logger.log(`[DEBUG] top10_arr is already an array with ${value.length} items`);
            if (value.length > 0 && value[0] && value[0].v && value[0].v.f) {
              Logger.log(`[DEBUG] Array items have nested structure, extracting...`);
              let arrayFieldSchema = null;
              for (let j = 0; j < fields.length; j++) {
                if (fields[j].name === 'top10_arr' && fields[j].fields) {
                  arrayFieldSchema = fields[j].fields;
                  Logger.log(`[DEBUG] Found array field schema with ${arrayFieldSchema.length} fields`);
                  break;
                }
              }

              const extracted = [];
              for (let idx = 0; idx < value.length; idx++) {
                const item = value[idx];
                if (item && item.v && item.v.f && Array.isArray(item.v.f)) {
                  const obj = {};
                  if (arrayFieldSchema) {
                    for (let k = 0; k < arrayFieldSchema.length && k < item.v.f.length; k++) {
                      const nestedFieldName = arrayFieldSchema[k].name;
                      let fieldValue = item.v.f[k].v;
                      obj[nestedFieldName] = fieldValue;
                    }
                  } else {
                    Logger.log(`[WARN] No schema found for array fields`);
                    for (let k = 0; k < item.v.f.length; k++) {
                      obj[`field_${k}`] = item.v.f[k].v;
                    }
                  }
                  extracted.push(obj);
                } else {
                  extracted.push(item);
                }
              }
              value = extracted;
              Logger.log(`[DEBUG] Extracted ${value.length} items from nested structure`);
              if (value.length > 0) {
                Logger.log(`[DEBUG] First extracted item keys: ${Object.keys(value[0]).join(', ')}`);
              }
            }
          } else if (value.f && Array.isArray(value.f)) {
            Logger.log(`[DEBUG] top10_arr has .f structure with ${value.f.length} rows`);
            let arrayFieldSchema = null;
            for (let j = 0; j < fields.length; j++) {
              if (fields[j].name === 'top10_arr' && fields[j].fields) {
                arrayFieldSchema = fields[j].fields;
                Logger.log(`[DEBUG] Found array field schema with ${arrayFieldSchema.length} fields`);
                break;
              }
            }

            const arr = [];
            for (let r = 0; r < value.f.length; r++) {
              const rowValue = value.f[r];
              if (rowValue && rowValue.v) {
                if (rowValue.v.f && Array.isArray(rowValue.v.f)) {
                  const obj = {};
                  if (arrayFieldSchema) {
                    for (let k = 0; k < arrayFieldSchema.length && k < rowValue.v.f.length; k++) {
                      const nestedFieldName = arrayFieldSchema[k].name;
                      let fieldValue = rowValue.v.f[k].v;

                      if (fieldValue && typeof fieldValue === 'object' && fieldValue.f) {
                        fieldValue = fieldValue.f.map(f => f.v);
                      }

                      obj[nestedFieldName] = fieldValue;
                    }
                  } else {
                    Logger.log(`[WARN] No schema found, attempting direct extraction`);
                    for (let k = 0; k < rowValue.v.f.length; k++) {
                      const field = rowValue.v.f[k];
                      if (field && field.v !== undefined) {
                        obj[`field_${k}`] = field.v;
                      }
                    }
                  }
                  arr.push(obj);
                } else {
                  arr.push(rowValue.v);
                }
              } else if (rowValue) {
                arr.push(rowValue);
              }
            }
            value = arr;
            Logger.log(`[DEBUG] Converted BigQuery .f structure to array: ${value.length} items`);
            if (value.length > 0) {
              Logger.log(`[DEBUG] First item keys: ${Object.keys(value[0]).join(', ')}`);
              Logger.log(`[DEBUG] First item preview: ${JSON.stringify(value[0]).substring(0, 200)}`);
            }
          } else if (value.v && Array.isArray(value.v)) {
            value = value.v;
            Logger.log(`[DEBUG] Extracted array from top10_arr.v: ${value.length} items`);
          } else if (value.arr && Array.isArray(value.arr)) {
            value = value.arr;
            Logger.log(`[DEBUG] Extracted array from top10_arr.arr: ${value.length} items`);
          } else {
            const keys = Object.keys(value);
            Logger.log(`[DEBUG] top10_arr object keys: ${keys.join(', ')}`);
            const numericKeys = keys.filter(k => !isNaN(Number(k))).map(Number).sort((a, b) => a - b);
            if (numericKeys.length > 0) {
              const arr = numericKeys.map(k => value[k]).filter(x => x !== undefined);
              value = arr;
              Logger.log(`[DEBUG] Converted object with numeric keys to array: ${value.length} items`);
            } else {
              Logger.log(`[DEBUG] Treating object as single-item array`);
              value = [value];
            }
          }
        } catch (e) {
          Logger.log(`[ERROR] Error processing top10_arr: ${e.message}`);
          Logger.log(`[ERROR] top10_arr structure: ${JSON.stringify(value).substring(0, 500)}`);
        }
      }
    }

    if (fieldName === 'top10_json' && value && typeof value === 'string') {
      const trimmed = value.trim();
      if (trimmed.startsWith('"') && trimmed.endsWith('"') && trimmed.length > 2) {
        try {
          const unquoted = JSON.parse(trimmed);
          if (typeof unquoted === 'string') {
            Logger.log(`[DEBUG] top10_json was double-encoded, unquoting once`);
            value = unquoted;
          }
        } catch (e) {
          Logger.log(`[DEBUG] top10_json unquoting failed, keeping original: ${e.message}`);
        }
      }
    }

    out[fieldName] = value;
  }

  Logger.log(`[DEBUG] total_attempts(raw): ${out.total_attempts}`);
  Logger.log(`[DEBUG] top10_len(raw): ${out.top10_len}`);

  Logger.log(`[DEBUG] top10_arr(type): ${typeof out.top10_arr}`);
  Logger.log(`[DEBUG] top10_arr(is null): ${out.top10_arr === null}`);
  Logger.log(`[DEBUG] top10_arr(is undefined): ${out.top10_arr === undefined}`);
  Logger.log(`[DEBUG] top10_arr(is array): ${Array.isArray(out.top10_arr)}`);
  if (out.top10_arr) {
    if (Array.isArray(out.top10_arr)) {
      Logger.log(`[DEBUG] top10_arr length: ${out.top10_arr.length}`);
    } else if (typeof out.top10_arr === 'object') {
      Logger.log(`[DEBUG] top10_arr object keys: ${Object.keys(out.top10_arr).join(', ')}`);
      Logger.log(`[DEBUG] top10_arr object preview: ${JSON.stringify(out.top10_arr).substring(0, 200)}`);
    }
  }

  if (out.top10_json) {
    const top10JsonPreview = String(out.top10_json || "");
    Logger.log(`[DEBUG] top10_json(type): ${typeof out.top10_json}`);
    Logger.log(`[DEBUG] top10_json(preview, first 200 chars): ${top10JsonPreview.substring(0, 200)}`);
  }

  recomputeRiskMetricsInResult_(out);

  return out;
}

function getDatasetLocation_(projectId, datasetId) {
  try {
    const ds = BigQuery.Datasets.get(projectId, datasetId);
    return (ds && ds.location) ? String(ds.location).trim() : "EU";
  } catch (e) {
    return "EU";
  }
}

/**
 * Fallback function to fetch top 10 as separate rows if array extraction fails
 */
function fetchTop10Fallback_(projectId, location, windowStart, windowEnd) {
  const excludeTypologyClause = (CFG.EXCLUDE_TYPOLOGY !== null)
    ? `AND (r.typology IS NULL OR r.typology != ${CFG.EXCLUDE_TYPOLOGY})`
    : "";
  const tz = CFG.TIMEZONE;
  const tsField = CFG.PAYMENT_TS_FIELD;

  const sql = `
    WITH reservas_por_email AS (
      SELECT
        email,
        MIN(fechaReserva) AS min_fecha_reserva_email,
        COUNTIF(estado = 4) > 0 AS reserva_previa_opinada_flag
      FROM \`${CFG.RESERVAS_TABLE}\`
      GROUP BY email
    ),
    base AS (
      SELECT
        pt.pnr,
        CAST(pt.transaction_id AS STRING) AS transaction_id,
        pt.${tsField} AS attempt_dt,
        TIMESTAMP(pt.${tsField}, '${tz}') AS attempt_ts,
        FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%S', TIMESTAMP(pt.${tsField}, '${tz}')) AS attempt_ts_iso,
        EXTRACT(HOUR FROM TIMESTAMP(pt.${tsField}, '${tz}')) AS hour_of_day,
        EXTRACT(DAYOFWEEK FROM TIMESTAMP(pt.${tsField}, '${tz}')) AS day_of_week,
        pt.gateway,
        pt.result,
        pt.amount,
        pt.currency,
        pt.card_country,
        pt.issuercountry,
        pt.countrycode,
        pt.cardbin,
        COALESCE(pt.geo_ip, '') AS geo_ip,
        r.pais_cliente,
        r.id_producto,
        r.product_type,
        r.canal,
        COALESCE(r.email, 'N/A') AS email,
        r.nombre AS nombre,
        r.apellidos AS apellidos,
        COALESCE(r.precioinicialeuros, pt.amount) AS amount_eur,
        DATE_DIFF(DATE(r.fecha), DATE(r.fechaReserva), DAY) AS booking_window_days,
        CAST(c.ratio_fraude AS INT64) AS ratio_fraude,
        pais.iso2 AS activity_country_abreviado,
        rpe.reserva_previa_opinada_flag,
        (r.fechaReserva IS NOT NULL AND rpe.min_fecha_reserva_email IS NOT NULL AND r.fechaReserva = rpe.min_fecha_reserva_email) AS is_first_booking,
        SAFE_CAST(acp.penalty AS INT64) AS penalty,
        COUNT(DISTINCT pt.transaction_id) OVER (PARTITION BY r.old_id, r.product_type) AS total_intentos_pago
      FROM \`${CFG.PAYMENTS_TABLE}\` pt
      LEFT JOIN \`${CFG.RESERVAS_TABLE}\` r ON r.cart_id = pt.pnr
      LEFT JOIN reservas_por_email rpe ON r.email = rpe.email
      LEFT JOIN \`${CFG.CATALOGO_TABLE}\` c ON r.id_producto = c.id_producto AND r.product_type = c.product_type
      LEFT JOIN \`${CFG.DESTINOS_TABLE}\` AS d ON c.id_ciudad = d.id
      LEFT JOIN \`${CFG.PAISES_TABLE}\` AS pais ON d.id_pais = pais.id
      LEFT JOIN \`${CFG.ACP_TABLE}\` AS acp ON c.id_producto = CAST(acp.activity_id AS STRING)
      WHERE pt.${tsField} >= DATETIME('${windowStart}')
        AND pt.${tsField} <= DATETIME('${windowEnd}')
        AND pt.payment_type = '${CFG.PAYMENT_TYPE_FILTER}'
        ${excludeTypologyClause}
    ),
    enriched AS (
      SELECT
        b.*,
        CASE WHEN booking_window_days IS NULL THEN 40
             WHEN booking_window_days <= 1 THEN 90
             WHEN booking_window_days <= 3 THEN 70
             WHEN booking_window_days <= 9 THEN 30 ELSE 0 END AS score_booking_window,
        CASE WHEN ratio_fraude IS NULL THEN 30
             WHEN ratio_fraude >= 999 THEN 90
             WHEN ratio_fraude >= 900 THEN 80
             WHEN ratio_fraude >= 800 THEN 70
             WHEN ratio_fraude >= 700 THEN 60
             WHEN ratio_fraude >= 600 THEN 50
             WHEN ratio_fraude >= 500 THEN 40
             WHEN ratio_fraude >= 400 THEN 20
             WHEN ratio_fraude >= 100 THEN 10
             ELSE 0
        END AS score_catalog_risk,
        CASE WHEN pais_cliente IS NULL OR TRIM(pais_cliente) = '' THEN 40
             WHEN UPPER(pais_cliente) IN ('ES','IT','FR') THEN 0
             WHEN UPPER(pais_cliente) IN ('AR','MX','CO','PE','UY','CR','BR') THEN 30
             ELSE 70 END AS score_country_risk
      FROM base b
    )
    SELECT * FROM enriched
    ORDER BY attempt_ts DESC
    LIMIT 500
  `;

  try {
    const res = BigQuery.Jobs.query({ query: sql, useLegacySql: false, location, timeoutMs: 30000 }, projectId);
    const jobId = res.jobReference.jobId;

    let poll = BigQuery.Jobs.getQueryResults(projectId, jobId, { location });
    let attempts = 0;
    while (!poll.jobComplete && attempts < 10) {
      Utilities.sleep(500);
      poll = BigQuery.Jobs.getQueryResults(projectId, jobId, { location });
      attempts++;
    }

    if (!poll.jobComplete || !poll.rows) return [];

    const fields = poll.schema.fields;
    const results = [];
    for (let i = 0; i < poll.rows.length; i++) {
      const row = poll.rows[i].f;
      const obj = {};
      for (let j = 0; j < fields.length; j++) {
        let v = row[j].v;
        const fname = fields[j].name;
        if (fname === "reserva_previa_opinada_flag" || fname === "is_first_booking") {
          obj[fname] = v === true || v === "true" || v === 1;
        } else {
          obj[fname] = v;
        }
      }
      results.push(obj);
    }

    const scored = results.map((r) => applyEjemploRiskToAttemptRow_(r));
    scored.sort((a, b) => Number(b.risk_score || 0) - Number(a.risk_score || 0));
    return scored.slice(0, CFG.TOP_N).map((r) => ({
      risk_score: r.risk_score,
      risk_tier: r.risk_tier,
      recommended_action: r.recommended_action,
      pnr: r.pnr,
      email: r.email,
      attempt_dt: r.attempt_dt != null ? String(r.attempt_dt) : "",
      gateway: r.gateway,
      result: r.result,
      amount: r.amount,
      currency: r.currency,
      card_country: r.card_country,
      issuercountry: r.issuercountry,
      countrycode: r.countrycode,
      cardbin: r.cardbin,
      pais_cliente: r.pais_cliente,
      id_producto: r.id_producto,
      product_type: r.product_type,
      ratio_fraude: r.ratio_fraude,
      booking_window_days: r.booking_window_days,
      score_booking_window: r.score_booking_window,
      score_catalog_risk: r.score_catalog_risk,
      score_country_risk: r.score_country_risk,
      motivos_puntuacion: r.motivos_puntuacion
    }));
  } catch (e) {
    Logger.log(`[ERROR] Fallback query error: ${e.message}`);
    return [];
  }
}

/* =========================
   ERROR (log only)
========================= */

function handleError_(error, runTsIso, execTimeMs) {
  Logger.log(
    `[ERROR] run_ts=${runTsIso} duration_ms=${execTimeMs} message=${error.message}\n${error.stack || "No stack trace"}`
  );
}

function mostrarDashboard() {
  try {
    escribirAnaliticaEnHoja();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const analyticsName = CFG.ANALYTICS_SHEET || "Analítica riesgo";
    const analyticsSheet = ss && ss.getSheetByName(analyticsName);
    if (analyticsSheet) {
      appendHistoricoTrafficSnapshotFromSheet_(ss, analyticsSheet, analyticsName);
    }
  } catch (e) {
    SpreadsheetApp.getUi().alert(
      "No se pudo escribir la analítica o el histórico de tráfico (el monitor se abrirá igual).\n\n" +
        (e.message || String(e))
    );
  }
  var html = HtmlService.createHtmlOutputFromFile('dashboard')
      .setWidth(1000)
      .setHeight(600);
  SpreadsheetApp.getUi().showModalDialog(html, 'Monitor de Tráfico en Tiempo Real');
}

function onOpen() {
  var ui = SpreadsheetApp.getUi();
  ui.createMenu('📊 Dashboard de Tráfico')
      .addItem('Abrir Monitor', 'mostrarDashboard')
      .addItem('Solo actualizar hoja analítica', 'escribirAnaliticaEnHoja')
      .addToUi();
}