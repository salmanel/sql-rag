import { loadSchemaAllowlist, renderAllowlistForPrompt } from "./allowlist";
import { queryAI } from "./query-ai";
import { query } from "./db";
import { buildChatSuggestions, SuggestionChip } from "./suggestions";
import {
  AllowlistTable,
  ChatLanguage,
  PlannerConcept,
  PlannerStatus,
  QueryPlan,
  SchemaAllowlist,
  compileQueryPlan,
  validatePlannerQueryPlan,
  validateAgainstAllowlist,
  validateQueryPlan,
} from "./query-plan";

interface ChatResponse {
  language: ChatLanguage;
  status?: PlannerStatus;
  answer: string;
  follow_up_question?: string;
  suggestions?: SuggestionChip[];
  conversation_status?: "normal" | "qualifying" | "lead_capture" | "appointment_ready";
  required_fields?: Array<"name" | "phone" | "email" | "availability">;
  projects?: ProjectCard[];
  queryPlan: QueryPlan;
  results: Record<string, unknown>[];
}

interface PlannerOutcome {
  executablePlan: QueryPlan;
  plannerLanguage: ChatLanguage;
  plannerStatus: PlannerStatus;
  askUser: string | null;
}

interface PresetExecution {
  plan: QueryPlan;
  rows: Record<string, unknown>[];
}

interface ProjectCard {
  id?: string;
  source_table?: string;
  project_id?: string;
  name: string;
  city?: string;
  description?: string;
  price_range?: string;
  images: string[];
}

interface LeadInfo {
  name?: string;
  phone?: string;
  email?: string;
  availability?: string;
  appointmentDateIso?: string;
}

interface ConversationState {
  language: ChatLanguage;
  mode: "normal" | "lead_capture";
  city?: string;
  budget?: number;
  bedrooms?: number;
  features: string[];
  selectedProject?: string;
  selectedProjectId?: string;
  selectedProjectTable?: string;
  selectedImmeubleId?: string;
  wantsVisit?: boolean;
  lead: LeadInfo;
}

const TECHNICAL_COLUMN_PATTERNS = [
  /^id$/i,
  /_id$/i,
  /uuid/i,
  /guid/i,
  /lat/i,
  /lng/i,
  /lon/i,
  /coord/i,
];

const KNOWN_CITY_TOKENS = new Set([
  "casablanca",
  "casa",
  "rabat",
  "zenata",
  "marrakech",
  "tanger",
  "tangier",
  "agadir",
]);

const DEBUG_SQL = process.env.DEBUG_SQL === "true";
const conversationStore = new Map<string, ConversationState>();
const EMPTY_PLAN: QueryPlan = {
  intent: { type: "lookup", table: "projects", columns: ["name"] },
  filters: [],
  limit: 10,
};

function logSql(tag: string, sql: string, params: unknown[], rows?: number): void {
  if (!DEBUG_SQL) return;
  console.log(`[SQL:${tag}] ${sql}`);
  console.log(`[SQL:${tag}:params] ${JSON.stringify(params)}`);
  if (typeof rows === "number") {
    console.log(`[SQL:${tag}:rows] ${rows}`);
  }
}

function getConversationState(sessionId: string, language: ChatLanguage): ConversationState {
  const existing = conversationStore.get(sessionId);
  if (existing) {
    return existing;
  }

  const fresh: ConversationState = {
    language,
    mode: "normal",
    features: [],
    lead: {},
  };
  conversationStore.set(sessionId, fresh);
  return fresh;
}

function isStrongEnglishMessage(message: string): boolean {
  const lower = message.toLowerCase();
  if (lower.trim().length < 4) return false;
  return includesAny(lower, [
    " i ",
    " i'm ",
    " i am ",
    " show me ",
    " what ",
    " where ",
    " which ",
    " budget ",
    " bedroom",
    " project",
    " please",
    " can you",
    " available",
  ]);
}

function isStrongFrenchMessage(message: string): boolean {
  const lower = message.toLowerCase();
  if (lower.trim().length < 3) return false;
  return includesAny(lower, [
    " je ",
    " j'",
    "vous",
    "projet",
    "ville",
    "budget",
    "chambre",
    "bonjour",
    "salut",
    "disponible",
    "pouvez-vous",
    "montre",
    "cherche",
  ]);
}

async function resolveChatLanguage(
  message: string,
  requestedLanguage: unknown,
  existingState: ConversationState | undefined,
): Promise<ChatLanguage> {
  const explicit = coerceLanguage(requestedLanguage);
  if (explicit) return explicit;

  if (!existingState) {
    return detectLanguage(message);
  }

  const english = isStrongEnglishMessage(` ${message} `);
  const french = isStrongFrenchMessage(` ${message} `);

  if (english && !french) return "en";
  if (french && !english) return "fr";
  return existingState.language;
}

function extractBudget(message: string): number | null {
  const normalized = message.toLowerCase().replace(/,/g, "").replace(/\s+/g, " ");
  const match = normalized.match(/(\d{3,7})(\s?(k|m|dh|mad))?/i);
  if (!match) return null;
  const raw = Number(match[1]);
  if (!Number.isFinite(raw)) return null;
  const unit = (match[3] || "").toLowerCase();
  if (unit === "k") return raw * 1000;
  if (unit === "m") return raw * 1000000;
  return raw;
}

function extractBedrooms(message: string): number | null {
  const normalized = message.toLowerCase();
  const match = normalized.match(/(\d+)\s*(bed|beds|bedroom|bedrooms|chambre|chambres|pieces|rooms)/i);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isInteger(value) && value > 0 && value < 10 ? value : null;
}

function extractFeatureTags(message: string): string[] {
  const lower = message.toLowerCase();
  const tags: string[] = [];
  if (includesAny(lower, ["garden", "jardin"])) tags.push("garden");
  if (includesAny(lower, ["pool", "piscine"])) tags.push("pool");
  if (includesAny(lower, ["secure", "securise", "sÃ©curisÃ©", "residence securisee", "rÃ©sidence sÃ©curisÃ©e"])) {
    tags.push("secure_residence");
  }
  return tags;
}

function detectVisitIntent(message: string): boolean {
  return includesAny(message.toLowerCase(), [
    "visit",
    "visite",
    "rendez-vous",
    "rendez vous",
    "rdv",
    "appointment",
    "prendre rendez",
    "book",
    "reserve",
    "rÃ©server",
    "planifier",
    "schedule",
    "see this project",
    "interested",
    "interesse",
    "intÃ©ressÃ©",
  ]);
}

function detectAffirmativeMessage(message: string): boolean {
  const normalized = normalizeForParsing(message).trim();
  return [
    "ok",
    "okay",
    "oui",
    "yes",
    "daccord",
    "d accord",
    "prenons un rendez vous",
    "prendre rendez vous",
    "on y va",
    "allons y",
  ].includes(normalized);
}

function detectBroadHousingIntent(message: string): boolean {
  return includesAny(message.toLowerCase(), [
    "project",
    "projects",
    "projet",
    "projets",
    "apartment",
    "appartement",
    "immeuble",
    "residence",
    "rÃ©sidence",
  ]);
}

function detectExplicitProjectsOnly(message: string): boolean {
  return includesAny(normalizeForParsing(message), ["project", "projects", "projet", "projets", "programme"]);
}

function detectExplicitApartmentsOnly(message: string): boolean {
  return includesAny(normalizeForParsing(message), [
    "apartment",
    "apartments",
    "appartement",
    "appartements",
    "appart",
    "immeuble",
    "immeubles",
    "unit",
    "units",
  ]);
}

function shouldSearchProjectsAndApartments(message: string): boolean {
  const normalized = normalizeForParsing(message);
  const explicitProjects = detectExplicitProjectsOnly(message);
  const explicitApartments = detectExplicitApartmentsOnly(message);
  const genericHousing = includesAny(normalized, [
    "bien",
    "biens",
    "offre",
    "offres",
    "logement",
    "logements",
    "property",
    "properties",
    "real estate",
    "immobilier",
    "ce que vous avez",
    "disponibilites",
    "disponibilite",
  ]);

  if (explicitProjects && explicitApartments) return true;
  if (!explicitProjects && !explicitApartments && genericHousing) return true;
  return false;
}

function buildQualificationQuestion(language: ChatLanguage, field: "city" | "budget" | "bedrooms" | "features"): string {
  if (language === "fr") {
    if (field === "city") return "Dans quelle ville cherchez-vous votre bien ?";
    if (field === "budget") return "Quel est votre budget approximatif ?";
    if (field === "bedrooms") return "Vous souhaitez combien de chambres ?";
    return "Avez-vous des criteres specifiques (jardin, piscine, residence securisee) ?";
  }

  if (field === "city") return "Which city are you looking in?";
  if (field === "budget") return "What is your approximate budget?";
  if (field === "bedrooms") return "How many bedrooms do you need?";
  return "Do you have preferred features (garden, pool, secure residence)?";
}

function buildQualificationChips(language: ChatLanguage, field: "city" | "budget" | "bedrooms" | "features"): SuggestionChip[] {
  if (field === "city") {
    return language === "fr"
      ? [
          { label: "Casablanca", payload: "projets a casablanca", type: "city" },
          { label: "Rabat", payload: "projets a rabat", type: "city" },
        ]
      : [
          { label: "Casablanca", payload: "projects in casablanca", type: "city" },
          { label: "Rabat", payload: "projects in rabat", type: "city" },
        ];
  }
  if (field === "budget") {
    return language === "fr"
      ? [
          { label: "< 1M MAD", payload: "budget 1000000 mad", type: "budget" },
          { label: "1M - 2M MAD", payload: "budget 1500000 mad", type: "budget" },
        ]
      : [
          { label: "< 1M MAD", payload: "budget 1000000 mad", type: "budget" },
          { label: "1M - 2M MAD", payload: "budget 1500000 mad", type: "budget" },
        ];
  }
  if (field === "bedrooms") {
    return [
      { label: "2 bedrooms", payload: "2 bedrooms", type: "bedrooms" },
      { label: "3 bedrooms", payload: "3 bedrooms", type: "bedrooms" },
    ];
  }
  return language === "fr"
    ? [
        { label: "Jardin", payload: "je veux jardin", type: "feature" },
        { label: "Piscine", payload: "je veux piscine", type: "feature" },
        { label: "Residence securisee", payload: "je veux residence securisee", type: "feature" },
      ]
    : [
        { label: "Garden", payload: "i want garden", type: "feature" },
        { label: "Pool", payload: "i want pool", type: "feature" },
        { label: "Secure residence", payload: "i want secure residence", type: "feature" },
      ];
}

function requiredQualificationField(state: ConversationState): "city" | "budget" | "bedrooms" | "features" | null {
  if (!state.city) return "city";
  if (!state.budget) return "budget";
  if (!state.bedrooms) return "bedrooms";
  if (state.features.length === 0) return "features";
  return null;
}

function buildPostResultFollowUp(
  language: ChatLanguage,
  message: string,
  state: ConversationState,
  hasRows: boolean,
): { followUpQuestion?: string; conversationStatus: ChatResponse["conversation_status"] } {
  let followUpQuestion: string | undefined;
  let conversationStatus: ChatResponse["conversation_status"] = "normal";

  if (!hasRows) {
    return { followUpQuestion, conversationStatus };
  }

  if (state.wantsVisit || detectVisitIntent(message)) {
    state.mode = "lead_capture";
    followUpQuestion =
      language === "fr"
        ? "Souhaitez-vous planifier une visite sur place ?"
        : "Would you like to schedule an on-site visit?";
    conversationStatus = "lead_capture";
    return { followUpQuestion, conversationStatus };
  }

  if (detectBroadHousingIntent(message)) {
    const nextField = requiredQualificationField(state);
    if (nextField && nextField !== "city") {
      followUpQuestion = buildQualificationQuestion(language, nextField);
      conversationStatus = "qualifying";
      return { followUpQuestion, conversationStatus };
    }
  }

  followUpQuestion =
    language === "fr"
      ? "Si vous souhaitez plus d'informations ou une visite sur place, nous pouvons planifier un rendez-vous avec l'un de nos agents."
      : "If you need more information or a live tour, we can schedule an appointment with one of our agents.";
  return { followUpQuestion, conversationStatus };
}

function extractLeadName(message: string): string | null {
  const cleaned = message.trim().replace(/\s+/g, " ");
  if (!cleaned) return null;

  const explicit =
    cleaned.match(/(?:my name is|i am|je m'appelle|je suis)\s+([A-Za-z\u00C0-\u017F' -]{4,60})/i) ||
    cleaned.match(/^(?:nom complet|full name)\s*[:\-]\s*([A-Za-z\u00C0-\u017F' -]{4,60})$/i);
  const candidate = (explicit?.[1] || cleaned).trim();
  const lowered = normalizeForParsing(candidate);

  const bannedTokens = [
    "rendez",
    "visite",
    "appointment",
    "possible",
    "ok",
    "bonjour",
    "salut",
    "merci",
    "demain",
    "lundi",
    "mardi",
    "mercredi",
    "jeudi",
    "vendredi",
    "samedi",
    "dimanche",
    "email",
    "telephone",
    "phone",
  ];
  if (bannedTokens.some((t) => lowered.includes(t))) return null;

  const parts = candidate.split(" ").filter((p) => /^[A-Za-z\u00C0-\u017F'-]{2,}$/.test(p));
  if (parts.length < 2 || parts.length > 4) return null;

  const normalized = parts.join(" ");
  return normalized.length >= 5 && normalized.length <= 60 ? normalized : null;
}

function extractLeadPhone(message: string): string | null {
  const match = message.match(/(\+?\d[\d\s-]{7,}\d)/);
  if (!match) return null;
  const normalized = match[1].replace(/\s+/g, " ").trim();
  const digits = normalized.replace(/\D/g, "");
  if (digits.length < 9 || digits.length > 15) return null;
  return normalized;
}

function extractLeadEmail(message: string): string | null {
  const match = message.match(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i);
  return match ? match[0].trim() : null;
}

function parseHourMinute(text: string): { hour: number; minute: number } | null {
  const hm = text.match(/\b(\d{1,2})(?::(\d{2}))?\s*(h|heure|heures)?\b/i);
  if (!hm) return null;
  const hour = Number(hm[1]);
  const minute = hm[2] ? Number(hm[2]) : 0;
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return { hour, minute };
}

function parseFrenchOrEnglishWeekday(text: string): number | null {
  const lower = normalizeForParsing(text);
  const map: Array<{ terms: string[]; day: number }> = [
    { terms: ["lundi", "monday"], day: 1 },
    { terms: ["mardi", "tuesday"], day: 2 },
    { terms: ["mercredi", "wednesday"], day: 3 },
    { terms: ["jeudi", "thursday"], day: 4 },
    { terms: ["vendredi", "friday"], day: 5 },
    { terms: ["samedi", "saturday"], day: 6 },
    { terms: ["dimanche", "sunday"], day: 0 },
  ];

  for (const entry of map) {
    if (entry.terms.some((t) => lower.includes(t))) return entry.day;
  }
  return null;
}

function getNextWeekdayDate(targetDay: number): Date {
  const now = new Date();
  const date = new Date(now);
  date.setHours(0, 0, 0, 0);
  const diff = (targetDay - date.getDay() + 7) % 7 || 7;
  date.setDate(date.getDate() + diff);
  return date;
}

function parseAvailabilityCandidate(message: string): Date | null {
  const trimmed = message.trim();
  if (!trimmed) return null;

  const direct = Date.parse(trimmed);
  if (Number.isFinite(direct)) {
    return new Date(direct);
  }

  const frDateTime = trimmed.match(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b(?:\s+|.*?\b)(\d{1,2})(?::(\d{2}))?/);
  if (frDateTime) {
    const day = Number(frDateTime[1]);
    const month = Number(frDateTime[2]) - 1;
    const year = Number(frDateTime[3]);
    const hour = Number(frDateTime[4]);
    const minute = frDateTime[5] ? Number(frDateTime[5]) : 0;
    const dt = new Date(year, month, day, hour, minute, 0, 0);
    if (Number.isFinite(dt.getTime())) return dt;
  }

  const weekday = parseFrenchOrEnglishWeekday(trimmed);
  const hm = parseHourMinute(trimmed);
  if (weekday !== null && hm) {
    const next = getNextWeekdayDate(weekday);
    next.setHours(hm.hour, hm.minute, 0, 0);
    return next;
  }

  return null;
}

function isAllowedAppointmentSlot(date: Date): boolean {
  const day = date.getDay(); // 0=sun, 6=sat
  if (day === 0 || day === 6) return false;

  const minutes = date.getHours() * 60 + date.getMinutes();
  const morningStart = 9 * 60;
  const morningEnd = 12 * 60;
  const afternoonStart = 14 * 60;
  const afternoonEnd = 18 * 60;

  const inMorning = minutes >= morningStart && minutes <= morningEnd;
  const inAfternoon = minutes >= afternoonStart && minutes <= afternoonEnd;
  return inMorning || inAfternoon;
}

function extractLeadAvailability(message: string): { raw: string; date: Date } | null {
  if (
    !includesAny(normalizeForParsing(message), [
      "available",
      "disponible",
      "tomorrow",
      "demain",
      "monday",
      "lundi",
      "tuesday",
      "mardi",
      "wednesday",
      "mercredi",
      "thursday",
      "jeudi",
      "friday",
      "vendredi",
      "saturday",
      "samedi",
      "sunday",
      "dimanche",
      "h",
      ":",
      "/",
    ])
  ) {
    return null;
  }
  const parsed = parseAvailabilityCandidate(message);
  if (!parsed) return null;
  return { raw: message.trim(), date: parsed };
}

function splitFullName(fullName: string): { firstName: string; lastName: string } {
  const cleaned = fullName.trim().replace(/\s+/g, " ");
  const parts = cleaned.split(" ");
  if (parts.length <= 1) {
    return { firstName: cleaned, lastName: "" };
  }
  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
  };
}

function parseAppointmentDate(raw: string | undefined): Date {
  if (!raw) return new Date();
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) return new Date();
  return new Date(parsed);
}

async function resolveProjectIdByName(projectName: string): Promise<string | null> {
  const exactSql = `SELECT TOP (1) [Id] FROM [dbo].[Projects] WHERE LOWER(CAST([Name] AS NVARCHAR(4000))) = LOWER(CAST(@p0 AS NVARCHAR(4000)));`;
  let result = await query(exactSql, [projectName.trim()]);
  if (!result.rows?.length) {
    const likeSql = `SELECT TOP (1) [Id] FROM [dbo].[Projects] WHERE LOWER(CAST([Name] AS NVARCHAR(4000))) LIKE LOWER(CAST(@p0 AS NVARCHAR(4000))) ORDER BY LEN(CAST([Name] AS NVARCHAR(4000))) ASC;`;
    result = await query(likeSql, [`%${projectName.trim()}%`]);
  }
  const value = result.rows?.[0]?.Id;
  if (typeof value === "string" && value.trim()) return value.trim();
  return null;
}

async function resolveImmeubleByName(
  immeubleName: string,
): Promise<{ immeubleId: string | null; projectId: string | null }> {
  const exactSql = `SELECT TOP (1) [Id], [ProjectId] FROM [dbo].[Immeubles] WHERE LOWER(CAST([Name] AS NVARCHAR(4000))) = LOWER(CAST(@p0 AS NVARCHAR(4000)));`;
  let result = await query(exactSql, [immeubleName.trim()]);
  if (!result.rows?.length) {
    const likeSql = `SELECT TOP (1) [Id], [ProjectId] FROM [dbo].[Immeubles] WHERE LOWER(CAST([Name] AS NVARCHAR(4000))) LIKE LOWER(CAST(@p0 AS NVARCHAR(4000))) ORDER BY LEN(CAST([Name] AS NVARCHAR(4000))) ASC;`;
    result = await query(likeSql, [`%${immeubleName.trim()}%`]);
  }

  const row = result.rows?.[0];
  const immeubleRaw = row?.Id;
  const projectRaw = row?.ProjectId;
  const immeubleId = typeof immeubleRaw === "string" && immeubleRaw.trim() ? immeubleRaw.trim() : null;
  const projectId = typeof projectRaw === "string" && projectRaw.trim() ? projectRaw.trim() : null;

  return {
    immeubleId,
    projectId,
  };
}

async function persistAppointment(state: ConversationState): Promise<void> {
  const fullName = state.lead.name?.trim() || "Client";
  const { firstName, lastName } = splitFullName(fullName);
  const appointmentDate = state.lead.appointmentDateIso
    ? new Date(state.lead.appointmentDateIso)
    : parseAppointmentDate(state.lead.availability);

  let projectId: string | null = null;
  let immeubleId: string | null = null;

  if (state.selectedProjectTable === "immeubles") {
    immeubleId = state.selectedImmeubleId ?? state.selectedProjectId ?? null;
    projectId = state.selectedProjectId ?? null;
    if ((immeubleId === null || projectId === null) && state.selectedProject) {
      const resolved = await resolveImmeubleByName(state.selectedProject);
      if (immeubleId === null) immeubleId = resolved.immeubleId;
      if (projectId === null) projectId = resolved.projectId;
    }
  } else {
    projectId = state.selectedProjectId ?? null;
    if (projectId === null && state.selectedProject) {
      projectId = await resolveProjectIdByName(state.selectedProject);
    }
  }

  const appointmentId = crypto.randomUUID();

  const insertSql = `INSERT INTO [dbo].[Appointments]
([Id], [ProjectId], [AppointmentDate], [Name], [LastName], [Email], [PhoneNumber], [Status], [ImmeubleId], [PropertyType])
VALUES (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9);`;

  const insertParams: Array<string | number | boolean | Date | null> = [
    appointmentId,
    projectId,
    appointmentDate,
    firstName,
    lastName || null,
    state.lead.email || null,
    state.lead.phone || null,
    "Pending",
    immeubleId,
    "project",
  ];

  console.log("[appointments] SQL preview:", insertSql);
  console.log("[appointments] SQL params:", JSON.stringify(insertParams));
  await query(insertSql, insertParams);
}

async function runBroadInventorySearch(message: string): Promise<PresetExecution | null> {
  if (!shouldSearchProjectsAndApartments(message)) {
    return null;
  }

  const city = extractLikelyLocationTerm(message);
  const likeCity = city ? `%${city}%` : null;

  const projectSql = city
    ? `SELECT TOP (25)
        [Id],
        [Name],
        [Address],
        [Description],
        [Images],
        CAST(NULL AS NVARCHAR(100)) AS [ProjectId],
        'projects' AS [SourceTable]
      FROM [dbo].[Projects]
      WHERE LOWER(CAST([Address] AS NVARCHAR(4000))) LIKE LOWER(CAST(@p0 AS NVARCHAR(4000)))
      ORDER BY [Name] ASC;`
    : `SELECT TOP (25)
        [Id],
        [Name],
        [Address],
        [Description],
        [Images],
        CAST(NULL AS NVARCHAR(100)) AS [ProjectId],
        'projects' AS [SourceTable]
      FROM [dbo].[Projects]
      ORDER BY [Name] ASC;`;

  const immeubleSql = city
    ? `SELECT TOP (25)
        [Id],
        [Name],
        [Location] AS [Address],
        [Description],
        [Images],
        [ProjectId],
        [MinPrice],
        [MaxPrice],
        'immeubles' AS [SourceTable]
      FROM [dbo].[Immeubles]
      WHERE LOWER(CAST([Location] AS NVARCHAR(4000))) LIKE LOWER(CAST(@p0 AS NVARCHAR(4000)))
      ORDER BY [Name] ASC;`
    : `SELECT TOP (25)
        [Id],
        [Name],
        [Location] AS [Address],
        [Description],
        [Images],
        [ProjectId],
        [MinPrice],
        [MaxPrice],
        'immeubles' AS [SourceTable]
      FROM [dbo].[Immeubles]
      ORDER BY [Name] ASC;`;

  const projectResult = await query(projectSql, likeCity ? [likeCity] : []);
  logSql("broad-search:projects", projectSql, likeCity ? [likeCity] : [], projectResult.rows.length);
  const immeubleResult = await query(immeubleSql, likeCity ? [likeCity] : []);
  logSql("broad-search:immeubles", immeubleSql, likeCity ? [likeCity] : [], immeubleResult.rows.length);

  const rows = [...projectResult.rows, ...immeubleResult.rows] as Record<string, unknown>[];
  if (rows.length === 0) {
    return null;
  }

  return {
    plan: {
      intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "images"] },
      filters: city ? [{ field: "address", operator: "contains", value: city }] : [],
      limit: 50,
    },
    rows,
  };
}

function includesAny(text: string, terms: string[]): boolean {
  const lower = text.toLowerCase();
  return terms.some((term) => lower.includes(term));
}

function normalizeMessageForPlanner(message: string): string {
  let normalized = message.trim();

  // Light typo normalization for high-impact intent words.
  const replacements: Array<[RegExp, string]> = [
    [/\bwcount\b/gi, "count"],
    [/\bcoutn\b/gi, "count"],
    [/\bcout\b/gi, "count"],
    [/\bcmpt\b/gi, "count"],
    [/\bcombine\b/gi, "combien"],
    [/\bstart with\b/gi, "starts with"],
  ];

  for (const [pattern, value] of replacements) {
    normalized = normalized.replace(pattern, value);
  }

  return normalized;
}

function applyIntentAndOperatorHints(plan: QueryPlan, message: string): QueryPlan {
  const lower = message.toLowerCase();
  const countHint = includesAny(lower, ["count", "how many", "number of", "combien", "nombre"]);
  const startsWithHint = includesAny(lower, ["starts with", "start with", "commence par", "commen", "debute par"]);
  const containsHint = includesAny(lower, ["contains", "contain", "contient", "include", "includes"]);

  const next: QueryPlan = {
    ...plan,
    filters: [...plan.filters],
  };

  if (countHint) {
    next.intent = {
      ...next.intent,
      type: "count",
      aggregation: undefined,
    };
  }

  if (startsWithHint || containsHint) {
    next.filters = next.filters.map((filter) => {
      const isTextLike = typeof filter.value === "string";
      if (!isTextLike) {
        return filter;
      }

      if (startsWithHint) {
        return { ...filter, operator: "starts_with" };
      }

      if (containsHint && filter.operator !== "starts_with") {
        return { ...filter, operator: "contains" };
      }

      return filter;
    });
  }

  return next;
}

function shouldAutoListProjects(message: string): boolean {
  const lower = message.trim().toLowerCase();
  const projectOnlyPatterns = [
    /^projects?$/,
    /^project$/,
    /^projets?$/,
    /^projet$/,
    /^list( all)? projects?$/,
    /^show( me)? projects?$/,
    /^can you list( all)? projects?$/,
  ];
  return projectOnlyPatterns.some((pattern) => pattern.test(lower));
}

function applyConceptSelectionHints(
  plannerPlan: {
    select: PlannerConcept[];
    notes: string;
  },
  message: string,
): {
  select: PlannerConcept[];
  notes: string;
} {
  const lower = message.toLowerCase();
  const hinted = new Set<PlannerConcept>(plannerPlan.select);

  if (includesAny(lower, ["price", "prices", "prix", "tarif", "tarifs", "cost", "budget", "montant"])) {
    hinted.add("PRICE");
  }

  if (includesAny(lower, ["surface", "area", "sqm", "m2", "superficie"])) {
    hinted.add("SURFACE");
  }

  if (includesAny(lower, ["status", "state", "etat", "disponible", "disponibilite"])) {
    hinted.add("PROJECT_STATUS");
  }

  if (includesAny(lower, ["description", "details", "desc"])) {
    hinted.add("PROJECT_DESCRIPTION");
  }

  if (includesAny(lower, ["name", "nom", "project"])) {
    hinted.add("PROJECT_NAME");
  }

  return {
    select: Array.from(hinted),
    notes: `${plannerPlan.notes} | concept-hints-applied`,
  };
}

function hasColumn(columns: Set<string>, candidates: string[]): boolean {
  return candidates.some((candidate) => columns.has(candidate.toLowerCase()));
}

function findColumn(columns: Set<string>, candidates: string[]): string | null {
  for (const candidate of candidates) {
    if (columns.has(candidate.toLowerCase())) {
      return candidate.toLowerCase();
    }
  }
  return null;
}

function normalizeSemanticFilters(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan {
  if (plan.filters.length === 0) {
    return plan;
  }

  const normalized: QueryPlan = {
    ...plan,
    filters: [...plan.filters],
  };

  const cityTerms = ["city", "ville"];
  const addressTerms = ["address", "adresse", "localisation", "location"];
  const cityColumn = findColumn(availableColumns, ["city", "ville"]);
  const addressColumn = findColumn(availableColumns, ["address", "adresse", "full_address", "street_address"]);

  // Prefer specific city fields over generic location/address when user asks city-level questions.
  if (cityColumn && includesAny(message, cityTerms)) {
    normalized.filters = normalized.filters.map((filter) => {
      const field = filter.field.toLowerCase();
      if (["address", "adresse", "location", "full_address", "street_address"].includes(field)) {
        return { ...filter, field: cityColumn };
      }
      return filter;
    });
  } else if (addressColumn && includesAny(message, addressTerms)) {
    // Prefer address fields when user explicitly asks for address.
    normalized.filters = normalized.filters.map((filter) => {
      if (filter.field.toLowerCase() === "city") {
        return { ...filter, field: addressColumn };
      }
      return filter;
    });
  }

  // Remove duplicate semantic filters like city=casablanca and address=casablanca.
  const seen = new Set<string>();
  normalized.filters = normalized.filters.filter((filter) => {
    const key = `${String(filter.value).toLowerCase()}::${filter.operator}`;
    const field = filter.field.toLowerCase();
    const semanticField =
      cityColumn && [cityColumn, "address", "adresse", "location", "full_address", "street_address"].includes(field)
        ? cityColumn
        : field;
    const semanticKey = `${semanticField}::${key}`;
    if (seen.has(semanticKey)) {
      return false;
    }
    seen.add(semanticKey);
    return true;
  });

  return normalized;
}

function normalizeLocationTerm(term: string): string {
  const map: Record<string, string> = {
    casa: "casablanca",
    casaa: "casablanca",
    casablanca: "casablanca",
    rabat: "rabat",
    zenata: "zenata",
    tanger: "tanger",
    tangier: "tanger",
    marrakech: "marrakech",
    agadir: "agadir",
  };
  const key = term.trim().toLowerCase();
  return map[key] || key;
}

function normalizeForParsing(text: string): string {
  return text
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function detectPresetKind(
  message: string,
): "projects_available_city" | "projects_in_progress" | "three_bedroom" | "latest_listings" | null {
  const lower = normalizeForParsing(message);
  const hasProjectWord = /(project|projects|projet|projets)/.test(lower);

  const isAvailableCity =
    hasProjectWord &&
    (/(available|disponible|disponibles)/.test(lower) || /\b(in|a|au|dans)\b/.test(lower)) &&
    Boolean(extractLikelyLocationTerm(message));
  if (isAvailableCity) return "projects_available_city";

  const isInProgress =
    hasProjectWord && /(in progress|en cours|sur plan|ongoing|currently in progress)/.test(lower);
  if (isInProgress) return "projects_in_progress";

  const isThreeBedroom =
    /\b3\b/.test(lower) &&
    /(bedroom|bedrooms|chambre|chambres)/.test(lower) &&
    /(apartment|apartments|appartement|appartements|units|unites|unite)/.test(lower);
  if (isThreeBedroom) return "three_bedroom";

  const isLatestListings =
    /(latest listings|latest projects|newest projects|dernieres annonces|projets recents|latest listing)/.test(lower);
  if (isLatestListings) return "latest_listings";

  return null;
}

async function runPresetQuery(message: string): Promise<PresetExecution | null> {
  const preset = detectPresetKind(message);
  if (!preset) return null;

  if (preset === "projects_available_city") {
    const city = extractLikelyLocationTerm(message);
    if (!city) return null;
    const likeCity = `%${city}%`;

    const baseSql = `SELECT TOP (50)
      [Id], [Name], [Address], [Description], [Type], [StatusGlobal]
      FROM [dbo].[Projects]
      WHERE LOWER(CAST([Address] AS NVARCHAR(4000))) LIKE LOWER(CAST(@p0 AS NVARCHAR(4000)))`;

    const withAvailability = `${baseSql}
      AND (
        LOWER(CAST([Type] AS NVARCHAR(4000))) LIKE '%livraison%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%available%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%disponible%'
      )
      ORDER BY [Name] ASC;`;

    let result = await query(withAvailability, [likeCity]);
    logSql("preset:projects_available_city:strict", withAvailability, [likeCity], result.rows.length);

    if ((result.rows || []).length === 0) {
      const relaxed = `${baseSql} ORDER BY [Name] ASC;`;
      result = await query(relaxed, [likeCity]);
      logSql("preset:projects_available_city:relaxed", relaxed, [likeCity], result.rows.length);
    }

    return {
      plan: {
        intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal"] },
        filters: [{ field: "address", operator: "contains", value: city }],
        limit: 50,
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (preset === "projects_in_progress") {
    const sql = `SELECT TOP (50)
      [Id], [Name], [Address], [Description], [Type], [StatusGlobal], [OverAllProgress]
      FROM [dbo].[Projects]
      WHERE
        LOWER(CAST([Type] AS NVARCHAR(4000))) LIKE '%sur plan%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%progress%'
        OR LOWER(CAST([StatusGlobal] AS NVARCHAR(4000))) LIKE '%en cours%'
      ORDER BY [OverAllProgress] DESC, [Name] ASC;`;
    const result = await query(sql, []);
    logSql("preset:projects_in_progress", sql, [], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal", "overallprogress"] },
        filters: [],
        limit: 50,
        sort: { field: "overallprogress", direction: "desc" },
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  if (preset === "three_bedroom") {
    const sql = `SELECT TOP (50)
      u.[Id] AS [UnitId],
      u.[NumberOfBedrooms],
      u.[NumberOfBathrooms],
      u.[TotalSurface],
      u.[LatestPrice],
      p.[Name] AS [ProjectName],
      p.[Address] AS [Address],
      i.[Name] AS [ImmeubleName],
      i.[MinPrice],
      i.[MaxPrice]
      FROM [dbo].[Units] u
      LEFT JOIN [dbo].[Projects] p ON p.[Id] = u.[ProjectId]
      LEFT JOIN [dbo].[Immeubles] i ON i.[ProjectId] = u.[ProjectId]
      WHERE u.[NumberOfBedrooms] = @p0
      ORDER BY u.[LatestPrice] ASC;`;
    const result = await query(sql, [3]);
    logSql("preset:three_bedroom", sql, [3], result.rows.length);
    return {
      plan: {
        intent: { type: "lookup", table: "units", columns: ["numberofbedrooms", "numberofbathrooms", "totalsurface", "latestprice"] },
        filters: [{ field: "numberofbedrooms", operator: "eq", value: 3 }],
        limit: 50,
      },
      rows: result.rows as Record<string, unknown>[],
    };
  }

  const latestSql = `SELECT TOP (50)
    [Id], [Name], [Address], [Description], [Type], [StatusGlobal]
    FROM [dbo].[Projects]
    ORDER BY [Id] DESC;`;
  const latestResult = await query(latestSql, []);
  logSql("preset:latest_listings", latestSql, [], latestResult.rows.length);
  return {
    plan: {
      intent: { type: "lookup", table: "projects", columns: ["name", "address", "description", "type", "statusglobal"] },
      filters: [],
      limit: 50,
      sort: { field: "id", direction: "desc" },
    },
    rows: latestResult.rows as Record<string, unknown>[],
  };
}

function removeNoisyNameFilters(plan: QueryPlan, availableColumns: Set<string>): QueryPlan {
  const nameCandidates = ["name", "project_name", "title", "nom"];
  const nameColumns = new Set(
    nameCandidates.filter((candidate) => availableColumns.has(candidate)).map((v) => v.toLowerCase()),
  );
  const genericValues = new Set(["project", "projects", "projet", "projets", "realestate", "residence", "residences"]);

  const cleaned = plan.filters.filter((filter) => {
    const isNameField = nameColumns.has(filter.field.toLowerCase());
    const isGenericValue = typeof filter.value === "string" && genericValues.has(filter.value.trim().toLowerCase());
    return !(isNameField && isGenericValue);
  });

  return {
    ...plan,
    filters: cleaned,
  };
}

function removeAmbiguousAvailabilityFilters(plan: QueryPlan, availableColumns: Set<string>): QueryPlan {
  const statusCandidates = ["status", "state", "etat", "availability", "disponibilite"];
  const statusColumns = new Set(
    statusCandidates.filter((candidate) => availableColumns.has(candidate)).map((v) => v.toLowerCase()),
  );

  const ambiguousStatusValues = new Set([
    "available",
    "availability",
    "disponible",
    "disponibilite",
    "available projects",
  ]);

  const filtered = plan.filters.filter((filter) => {
    const isStatusField = statusColumns.has(filter.field.toLowerCase());
    if (!isStatusField || typeof filter.value !== "string") return true;
    const value = filter.value.trim().toLowerCase();
    return !ambiguousStatusValues.has(value);
  });

  return {
    ...plan,
    filters: filtered,
  };
}

function enforceLocationFilterFromMessage(plan: QueryPlan, availableColumns: Set<string>, message: string): QueryPlan {
  if (plan.intent.type !== "lookup") return plan;
  const rawTerm = extractLikelyLocationTerm(message);
  if (!rawTerm) return plan;
  const term = normalizeLocationTerm(rawTerm);

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) return plan;
  const addressPriority = ["address", "adresse", "full_address", "street_address"];
  const addressColumn = addressPriority.find((col) => availableColumns.has(col));
  const primaryColumn = addressColumn || locationColumns[0];
  const useContains = addressPriority.includes(primaryColumn);

  const hasLocationFilter = plan.filters.some((f) => hasLocationLikeColumn(f.field));
  if (hasLocationFilter) {
    return {
      ...plan,
      filters: plan.filters.map((f) => {
        if (!hasLocationLikeColumn(f.field)) return f;
        if (typeof f.value !== "string") return f;
        return {
          ...f,
          field: primaryColumn,
          operator: useContains ? "contains" : "eq",
          value: normalizeLocationTerm(f.value),
        };
      }),
    };
  }

  return {
    ...plan,
    filters: [
      ...plan.filters,
      {
        field: primaryColumn,
        operator: useContains ? "contains" : "eq",
        value: term,
      },
    ],
  };
}

function applyDirectProjectLocationOverride(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan {
  const lower = message.toLowerCase().trim();
  const looksLikeProjectLocationQuery =
    /(project|projects|projet|projets)/.test(lower) &&
    /\b(in|a|au|Ã |dans)\b/.test(lower);

  if (!looksLikeProjectLocationQuery) {
    return plan;
  }

  const rawTerm = extractLikelyLocationTerm(message);
  if (!rawTerm) {
    return plan;
  }

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) {
    return plan;
  }

  const addressPriority = ["address", "adresse", "full_address", "street_address"];
  const addressColumn = addressPriority.find((col) => availableColumns.has(col));
  const primaryColumn = addressColumn || locationColumns[0];
  const term = normalizeLocationTerm(rawTerm);

  // Deterministic override for broad queries: keep only location filter.
  return {
    ...plan,
    filters: [
      {
        field: primaryColumn,
        operator: "contains",
        value: term,
      },
    ],
  };
}

function hasLocationLikeColumn(column: string): boolean {
  return /(city|ville|address|adresse|location|quartier|zone|secteur|localisation)/i.test(column);
}

function isGeoLikeColumn(column: string): boolean {
  return /(lat|lng|lon|coord|latitude|longitude)/i.test(column) || /^location$/i.test(column);
}

function rankLocationColumns(columns: string[]): string[] {
  const score = (col: string): number => {
    const lower = col.toLowerCase();
    let s = 0;
    if (/city|ville/.test(lower)) s += 100;
    if (/address|adresse|street_address|full_address/.test(lower)) s += 90;
    if (/quartier|zone|secteur|localisation/.test(lower)) s += 75;
    if (/location/.test(lower)) s += 40;
    if (isGeoLikeColumn(lower)) s -= 60;
    return s;
  };
  return [...columns].sort((a, b) => score(b) - score(a));
}

function extractLikelyLocationTerm(message: string): string | null {
  const cleaned = normalizeForParsing(message)
    .replace(/[^a-z0-9\u00c0-\u017f\s-]/gi, " ")
    .trim();
  if (!cleaned) return null;

  const words = cleaned.split(/\s+/).filter(Boolean);
  for (const word of words) {
    if (KNOWN_CITY_TOKENS.has(word)) {
      return normalizeLocationTerm(word);
    }
  }

  const prepositions = new Set(["in", "a", "au", "dans", "near", "pres", "de"]);
  const stopwords = new Set([
    "what",
    "about",
    "location",
    "city",
    "ville",
    "where",
    "in",
    "at",
    "de",
    "des",
    "du",
    "la",
    "le",
    "les",
    "au",
    "aux",
    "dans",
    "projects",
    "project",
    "projets",
    "projet",
    "known",
    "available",
    "show",
    "list",
    "have",
    "you",
    "your",
    "the",
    "all",
    "are",
    "is",
  ]);

  for (let i = 0; i < words.length - 1; i++) {
    if (!prepositions.has(words[i])) continue;
    const candidate = words[i + 1];
    if (candidate.length > 1 && !stopwords.has(candidate)) {
      return normalizeLocationTerm(candidate);
    }
  }

  if (words.length === 1 && KNOWN_CITY_TOKENS.has(words[0])) {
    return normalizeLocationTerm(words[0]);
  }

  return null;
}

function extractCityTokenForForcedProjects(message: string): string | null {
  return extractLikelyLocationTerm(message);
}

function getRankedLocationColumns(columns: Set<string>): string[] {
  return rankLocationColumns(Array.from(columns).filter(hasLocationLikeColumn));
}

function buildForcedProjectsAddressPlan(message: string, allowlist: SchemaAllowlist): QueryPlan | null {
  const lower = normalizeForParsing(message);
  const mentionsProject = /(project|projects|projet|projets)/.test(lower);
  const mentionsCountIntent = /\b(count|combien|nombre|how many)\b/.test(lower);
  const cityToken = extractCityTokenForForcedProjects(message);

  if (!mentionsProject && !cityToken) return null;
  if (mentionsCountIntent) return null;

  const directProjects = allowlist.tables.get("projects");
  const projectCandidates = Array.from(allowlist.tables.entries()).filter(([key]) => /project|projet/i.test(key));
  const structuralCandidates = Array.from(allowlist.tables.entries()).filter(([, table]) => {
    const cols = table.columns;
    const hasLocationLike = getRankedLocationColumns(cols).length > 0;
    const hasNameLike = ["name", "project_name", "title", "nom"].some((c) => cols.has(c));
    return hasLocationLike && hasNameLike;
  });

  const scoreTable = (key: string, table: AllowlistTable): number => {
    let score = 0;
    if (key === "projects") score += 100;
    if (key === "project") score += 90;
    if (/project|projet/.test(key)) score += 60;
    if (getRankedLocationColumns(table.columns).length > 0) score += 50;
    if (["name", "project_name", "title", "nom"].some((col) => table.columns.has(col))) score += 30;
    return score;
  };

  const allCandidates = [...projectCandidates, ...structuralCandidates].sort(
    (a, b) => scoreTable(b[0], b[1]) - scoreTable(a[0], a[1]),
  );
  const bestCandidateEntry = allCandidates[0];
  const projects = directProjects || (bestCandidateEntry ? bestCandidateEntry[1] : null);
  if (!projects) return null;

  const tableKey =
    directProjects
      ? "projects"
      : (bestCandidateEntry ? bestCandidateEntry[0] : "projects");

  const primaryLocationColumn = getRankedLocationColumns(projects.columns)[0];
  if (!primaryLocationColumn && cityToken) return null;

  const preferredColumns = [
    "name",
    "project_name",
    "title",
    ...(primaryLocationColumn ? [primaryLocationColumn] : []),
    "description",
    "status",
    "minprice",
    "maxprice",
  ].filter((col, idx, arr) => projects.columns.has(col) && arr.indexOf(col) === idx);

  return {
    intent: {
      type: "lookup",
      table: tableKey,
      columns: preferredColumns.length > 0 ? preferredColumns : undefined,
    },
    filters:
      cityToken && primaryLocationColumn
        ? [
            {
              field: primaryLocationColumn,
              operator: "contains",
              value: cityToken,
            },
          ]
        : [],
    limit: 50,
    sort: undefined,
  };
}

function buildCityFallbackPlans(message: string, allowlist: SchemaAllowlist): QueryPlan[] {
  const cityToken = extractCityTokenForForcedProjects(message);
  if (!cityToken) return [];

  const candidates = Array.from(allowlist.tables.entries())
    .filter(([, table]) => {
      const hasLocationLike = getRankedLocationColumns(table.columns).length > 0;
      const hasNameLike = ["name", "project_name", "title", "nom"].some((c) => table.columns.has(c));
      return hasLocationLike && hasNameLike;
    })
    .sort(([aKey], [bKey]) => {
      const score = (key: string): number => {
        if (key === "projects") return 100;
        if (key === "project") return 90;
        if (/project|projet/i.test(key)) return 70;
        return 10;
      };
      return score(bKey) - score(aKey);
    });

  const plans: QueryPlan[] = [];
  for (const [tableKey, tableMeta] of candidates) {
    const locationColumns = getRankedLocationColumns(tableMeta.columns);
    for (const locationColumn of locationColumns) {
      const preferredColumns = [
        "name",
        "project_name",
        "title",
        locationColumn,
        "description",
        "status",
        "minprice",
        "maxprice",
      ].filter((col, idx, arr) => tableMeta.columns.has(col) && arr.indexOf(col) === idx);

      plans.push({
        intent: {
          type: "lookup" as const,
          table: tableKey,
          columns: preferredColumns.length > 0 ? preferredColumns : undefined,
        },
        filters: [
          {
            field: locationColumn,
            operator: "contains" as const,
            value: cityToken,
          },
        ],
        limit: 50,
        sort: undefined,
      });
    }
  }

  return plans;
}

function detectProjectCityRequest(message: string): { city: string | null; projectIntent: boolean } {
  const city = extractCityTokenForForcedProjects(message);
  const lower = normalizeForParsing(message);
  const projectIntent = /(project|projects|projet|projets)/.test(lower) || Boolean(city);
  return { city, projectIntent };
}

function quoteIdent(identifier: string): string {
  return `[${identifier.replace(/]/g, "]]")}]`;
}

function quoteTable(schema: string, table: string): string {
  return `${quoteIdent(schema)}.${quoteIdent(table)}`;
}

async function runProjectCityRescueLookup(
  message: string,
  allowlist: SchemaAllowlist,
): Promise<{ plan: QueryPlan; rows: Record<string, unknown>[] } | null> {
  const intent = detectProjectCityRequest(message);
  if (!intent.projectIntent || !intent.city) return null;

  const candidates = Array.from(allowlist.tables.entries())
    .filter(([, table]) => {
      const hasNameLike = ["name", "project_name", "title", "nom"].some((c) => table.columns.has(c));
      const hasLocationLike = getRankedLocationColumns(table.columns).length > 0;
      return hasNameLike && hasLocationLike;
    })
    .sort(([aKey], [bKey]) => {
      const score = (key: string): number => {
        if (key === "projects") return 100;
        if (/project|projet/i.test(key)) return 70;
        return 10;
      };
      return score(bKey) - score(aKey);
    });

  for (const [tableKey, table] of candidates) {
    const locationCols = getRankedLocationColumns(table.columns).slice(0, 4);
    if (locationCols.length === 0) continue;

    const selectedColumns = [
      "name",
      "project_name",
      "title",
      ...locationCols,
      "description",
      "status",
      "minprice",
      "maxprice",
    ].filter((col, idx, arr) => table.columns.has(col) && arr.indexOf(col) === idx);

    const whereParts: string[] = [];
    const params: Array<string | number | boolean | Date | null> = [];
    for (const col of locationCols) {
      const ref = `@p${params.length}`;
      params.push(`%${intent.city}%`);
      whereParts.push(
        `LOWER(CAST(${quoteIdent(col)} AS NVARCHAR(4000))) LIKE LOWER(CAST(${ref} AS NVARCHAR(4000)))`,
      );
    }

    const sql = `SELECT TOP (50) ${selectedColumns.length > 0 ? selectedColumns.map(quoteIdent).join(", ") : "*"}
FROM ${quoteTable(table.schema, table.table)}
WHERE (${whereParts.join(" OR ")});`;

    const result = await query(sql, params);
    logSql(`rescue:${tableKey}`, sql, params, result.rows.length);
    const rows = dedupeRows(result.rows as Record<string, unknown>[]);
    if (rows.length === 0) continue;

    return {
      plan: {
        intent: {
          type: "lookup",
          table: tableKey,
          columns: selectedColumns.length > 0 ? selectedColumns : undefined,
        },
        filters: [
          {
            field: locationCols[0],
            operator: "contains",
            value: intent.city,
          },
        ],
        limit: 50,
        sort: undefined,
      },
      rows,
    };
  }

  return null;
}

function buildRelaxedLocationPlan(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan | null {
  if (plan.intent.type !== "lookup") return null;

  const locationColumns = rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn));
  if (locationColumns.length === 0) return null;

  const relaxed: QueryPlan = {
    ...plan,
    filters: [...plan.filters],
  };

  let changed = false;
  relaxed.filters = relaxed.filters.map((filter) => {
    if (hasLocationLikeColumn(filter.field) && filter.operator !== "contains") {
      changed = true;
      return { ...filter, operator: "contains" };
    }
    return filter;
  });

  if (relaxed.filters.length === 0) {
    const term = extractLikelyLocationTerm(message);
    if (term) {
      changed = true;
      relaxed.filters.push({
        field: locationColumns[0],
        operator: "contains",
        value: term,
      });
    }
  }

  return changed ? relaxed : null;
}

function getAlternativeLocationColumns(availableColumns: Set<string>, current: string): string[] {
  return rankLocationColumns(Array.from(availableColumns).filter(hasLocationLikeColumn)).filter(
    (col) => col.toLowerCase() !== current.toLowerCase(),
  );
}

function applyClientFriendlyProjection(
  plan: QueryPlan,
  availableColumns: Set<string>,
  message: string,
): QueryPlan {
  if (plan.intent.type !== "lookup") {
    return plan;
  }

  const explicitlyAsksTechnicalFields = includesAny(message, [
    "id",
    "uuid",
    "guid",
    "coord",
    "coordinate",
    "latitude",
    "longitude",
    "lat",
    "lng",
    "location",
  ]);

  if (explicitlyAsksTechnicalFields) {
    return plan;
  }

  const preferred = [
    "name",
    "project_name",
    "title",
    "address",
    "full_address",
    "street_address",
    "description",
    "details",
    "summary",
  ];

  const blockedPatterns = [
    /^id$/,
    /_id$/,
    /uuid/,
    /guid/,
    /lat/,
    /lng/,
    /lon/,
    /coord/,
    /^location$/,
  ];

  const selected = preferred.filter((column) => availableColumns.has(column));
  const currentlySelected = new Set((plan.intent.columns || []).map((c) => c.toLowerCase()));
  const shouldKeepPricing = includesAny(message, ["price", "prix", "tarif", "budget", "cost", "montant"]);
  const shouldKeepSurface = includesAny(message, ["surface", "area", "sqm", "m2", "superficie"]);
  const keepExtra = Array.from(availableColumns).filter((column) => {
    if (currentlySelected.has(column)) return true;
    if (shouldKeepPricing && /(price|prix|cost|amount|montant|budget|tarif|min|max)/i.test(column)) return true;
    if (shouldKeepSurface && /(surface|area|sqm|sqft|m2|superficie|min|max)/i.test(column)) return true;
    return false;
  });

  if (selected.length > 0) {
    return {
      ...plan,
      intent: {
        ...plan.intent,
        columns: Array.from(new Set([...selected, ...keepExtra])).slice(0, 10),
      },
    };
  }

  const fallbackColumns = Array.from(availableColumns).filter(
    (column) => !blockedPatterns.some((pattern) => pattern.test(column)),
  );

  if (fallbackColumns.length === 0) {
    return plan;
  }

  return {
    ...plan,
    intent: {
      ...plan.intent,
      columns: fallbackColumns.slice(0, 6),
    },
  };
}

function dedupeRows(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length <= 1) {
    return rows;
  }

  const keys = Object.keys(rows[0] || {});
  const hasName = keys.some((key) => key.toLowerCase() === "name");
  const hasDescription = keys.some((key) => key.toLowerCase() === "description");
  const preferredKeys =
    hasName && hasDescription
      ? keys.filter((key) => ["name", "description"].includes(key.toLowerCase()))
      : keys.filter((key) => !TECHNICAL_COLUMN_PATTERNS.some((pattern) => pattern.test(key)));

  const stableKeys = preferredKeys.length > 0 ? preferredKeys : keys;
  const seen = new Set<string>();

  return rows.filter((row) => {
    const fingerprint = stableKeys
      .map((key) => String(row[key] ?? "").trim().toLowerCase())
      .join("|");
    if (!fingerprint) {
      return true;
    }
    if (seen.has(fingerprint)) {
      return false;
    }
    seen.add(fingerprint);
    return true;
  });
}

function isTechnicalKey(key: string): boolean {
  const lower = key.toLowerCase();
  return (
    lower === "id" ||
    lower.endsWith("_id") ||
    lower.includes("uuid") ||
    lower.includes("guid") ||
    lower.includes("coord") ||
    lower.includes("lat") ||
    lower.includes("lng") ||
    lower.includes("lon")
  );
}

function sanitizeRowsForResponder(
  rows: Record<string, unknown>[],
  message: string,
): Record<string, unknown>[] {
  if (rows.length === 0) {
    return rows;
  }

  const asksTechnical = includesAny(message, [
    "id",
    "uuid",
    "guid",
    "coordinate",
    "coordinates",
    "location",
    "latitude",
    "longitude",
    "lat",
    "lng",
  ]);

  if (asksTechnical) {
    return rows;
  }

  return rows.map((row) => {
    const clean: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      if (!isTechnicalKey(key)) {
        clean[key] = value;
      }
    }
    return Object.keys(clean).length > 0 ? clean : row;
  });
}

function toFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function findKeyCaseInsensitive(row: Record<string, unknown>, candidates: string[]): string | null {
  const keys = Object.keys(row);
  for (const candidate of candidates) {
    const hit = keys.find((key) => key.toLowerCase() === candidate.toLowerCase());
    if (hit) return hit;
  }
  return null;
}

function enrichRowsWithPriceRange(
  rows: Record<string, unknown>[],
  message: string,
): Record<string, unknown>[] {
  const asksPrice = includesAny(message, ["price", "prices", "prix", "tarif", "tarifs", "budget", "cost", "montant"]);
  if (!asksPrice || rows.length === 0) return rows;

  return rows.map((row) => {
    const minKey = findKeyCaseInsensitive(row, ["minprice", "min_price", "price_min", "prixmin"]);
    const maxKey = findKeyCaseInsensitive(row, ["maxprice", "max_price", "price_max", "prixmax"]);
    if (!minKey || !maxKey) return row;

    const minValue = toFiniteNumber(row[minKey]);
    const maxValue = toFiniteNumber(row[maxKey]);
    if (minValue === null || maxValue === null) return row;

    const low = Math.min(minValue, maxValue);
    const high = Math.max(minValue, maxValue);
    return {
      ...row,
      price_range: `between ${low} and ${high}`,
    };
  });
}

function extractImageUrls(value: unknown): string[] {
  if (value === null || value === undefined) return [];
  const text = String(value).trim();
  if (!text) return [];

  // Handles single URL, comma-separated, or JSON-like arrays in a forgiving way.
  const candidates = text
    .replace(/[\[\]"]/g, "")
    .split(/[,\s]+/)
    .map((s) => s.trim())
    .filter(Boolean);

  return candidates.filter((s) => /^https?:\/\//i.test(s)).slice(0, 5);
}

function mapRowsToProjectCards(rows: Record<string, unknown>[], sourceTable?: string): ProjectCard[] {
  const cards: ProjectCard[] = [];
  const seen = new Set<string>();

  for (const row of rows) {
    const idRaw = firstExistingValue(row, ["id"]);
    const id = idRaw || undefined;
    const projectIdRaw = firstExistingValue(row, ["projectid"]);
    const projectId = projectIdRaw || undefined;
    const rowSourceTable = firstExistingValue(row, ["sourcetable"]) || sourceTable;
    const name =
      firstExistingValue(row, ["name", "projectname", "project_name", "title", "immeublename"]) || "Projet";
    const city = firstExistingValue(row, ["address", "city", "ville", "location"]) || undefined;
    const description = firstExistingValue(row, ["description"]) || undefined;
    const minPrice = firstExistingValue(row, ["minprice"]);
    const maxPrice = firstExistingValue(row, ["maxprice"]);
    const priceRange = firstExistingValue(row, ["price_range"]) || (minPrice && maxPrice ? `between ${minPrice} and ${maxPrice}` : undefined);
    const imageRaw = firstExistingValue(row, ["images", "imageprincipale"]);
    const images = imageRaw ? extractImageUrls(imageRaw) : [];

    const key = `${name.toLowerCase()}|${String(city || "").toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);

    cards.push({
      id,
      source_table: rowSourceTable,
      project_id: projectId,
      name,
      city,
      description,
      price_range: priceRange,
      images,
    });
  }

  return cards.slice(0, 6);
}

function looksLikeNoResultsAnswer(text: string): boolean {
  const lower = normalizeForParsing(text);
  return (
    lower.includes("no project") ||
    lower.includes("no projects") ||
    lower.includes("couldnt find") ||
    lower.includes("could not find") ||
    lower.includes("aucun projet") ||
    lower.includes("pas de projet") ||
    lower.includes("je nai pas trouve")
  );
}

function firstExistingValue(row: Record<string, unknown>, keys: string[]): string | null {
  for (const key of keys) {
    const actual = Object.keys(row).find((k) => k.toLowerCase() === key.toLowerCase());
    if (!actual) continue;
    const value = row[actual];
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (!text) continue;
    return text;
  }
  return null;
}

function buildDeterministicRowsAnswer(
  language: ChatLanguage,
  rows: Record<string, unknown>[],
): string {
  const picked = rows.slice(0, 5).map((row) => {
    const name = firstExistingValue(row, ["name", "project_name", "title"]) || "Project";
    const city = firstExistingValue(row, ["address", "city", "ville"]);
    const desc = firstExistingValue(row, ["description"]);
    const priceRange = firstExistingValue(row, ["price_range"]);

    const parts: string[] = [name];
    if (city) {
      parts.push(language === "fr" ? `a ${city}` : `in ${city}`);
    }
    if (priceRange) {
      parts.push(language === "fr" ? `prix ${priceRange}` : `price ${priceRange}`);
    }
    if (desc) {
      parts.push(desc);
    }
    return `- ${parts.join(": ")}`;
  });

  if (language === "fr") {
    return `Voici les projets trouves:\n${picked.join("\n")}`;
  }
  return `Here are the projects found:\n${picked.join("\n")}`;
}

function coerceLanguage(language: unknown): ChatLanguage | null {
  if (language === "en" || language === "fr") {
    return language;
  }
  return null;
}

async function detectLanguage(message: string): Promise<ChatLanguage> {
  const response = await queryAI(
    `Detect if the user message is French or English.
Return JSON only: {"language":"fr"} or {"language":"en"}.
If uncertain, return {"language":"en"}.`,
    message,
    true,
  );

  try {
    const parsed = JSON.parse(response) as { language?: string };
    return parsed.language === "fr" ? "fr" : "en";
  } catch {
    return "en";
  }
}

async function generateQueryPlan(
  message: string,
  language: ChatLanguage,
  allowlist: SchemaAllowlist,
): Promise<PlannerOutcome> {
  const normalizedMessage = normalizeMessageForPlanner(message);
  const schemaPrompt = renderAllowlistForPrompt(allowlist);
  const operators = Array.from(allowlist.operators).join(", ");
  const plannerSystemPrompt = `You are a planner for a bilingual (fr/en) real-estate sales assistant.
The assistant behaves like a real estate agent helping clients discover projects, qualify their needs, and organize visits.
Return ONLY valid JSON and no extra text.

Planner output schema:
{
  "language": "fr|en",
  "status": "ok|need_clarification|out_of_scope",
  "intent": "lookup|count|aggregate",
  "table": "<allowlisted table key>",
  "select": ["LOCATION_CITY|LOCATION_TEXT|PROJECT_NAME|PROJECT_DESCRIPTION|PROJECT_STATUS|PRICE|SURFACE"],
  "filters": [{"concept":"<same concept set>","operator":"=|!=|LIKE|IN","value":"string|string[]"}],
  "sort": [{"concept":"<same concept set>","direction":"asc|desc"}],
  "limit": 1..50,
  "aggregation": null | {"func":"sum|avg|min|max","concept":"PRICE|SURFACE|PROJECT_STATUS|PROJECT_NAME|PROJECT_DESCRIPTION","group_by":["<concept>", "..."]},
  "ask_user": null | "friendly follow-up question",
  "notes": "short note"
}

Rules:
- Detect language from user text and set language to fr or en.
- Tolerate typos/grammar issues.
- Use concept-first planning only. Never output raw column names.
- Use schema ConceptMap to decide concepts.
- LOCATION_CITY synonyms include: ville, city, casablanca, rabat, tanger, marrakech, agadir.
- LOCATION_CITY â†’ ["Ville", "City", "Location", "Adresse"]
- LOCATION_TEXT synonyms include: adresse, address, quartier, zone, localisation, location, secteur, pres de, near, by.
- Prefer LOCATION_CITY for city questions. Use LOCATION_TEXT for quartier/zone/near/by.
- Never invent table names or concepts.
- Don't mind case sensitivity
- filters must be [] when not needed.
- If the user mentions â€œprixâ€, â€œpriceâ€, â€œbudgetâ€, â€œcoÃ»tâ€, â€œcostâ€:
    Intent remains LOOKUP
    Include MinPrice and MaxPrice (or PRICE_RANGE concept)
    Do NOT switch to COUNT or AGGREGATE
- status=ok only when plan is executable.
- status=need_clarification when key info is missing or ambiguous; include ask_user in same detected language.
- status=out_of_scope when user asks outside real-estate data; include ask_user in same detected language.
- For intent=count, aggregation must be null.
- For intent=aggregate, aggregation is required.
- Deduplicate select concepts.
- Only ignore values like â€œtestâ€, â€œdummyâ€, â€œsampleâ€ WHEN they appear ALONE and NOT as part of a longer name.
  If â€œtestâ€ is part of a multi-word entity name (e.g., â€œlilas testâ€), treat it as a valid name.
- If both PROJECT_NAME and PROJECT_DESCRIPTION are redundant, keep PROJECT_NAME unless explicitly asked for description.
- Allowed SQL operators for later execution context are: ${operators}.`;

  const plannerUserPrompt = `Allowlisted tables, columns and concept maps:
${schemaPrompt}

User message:
${normalizedMessage}`;

  async function runPlanner(prompt: string): Promise<{ parsed: unknown; raw: string }> {
    const raw = await queryAI(plannerSystemPrompt, prompt, true);
    try {
      return { parsed: JSON.parse(raw), raw };
    } catch {
      // Let validation + repair flow handle malformed JSON.
      return { parsed: raw, raw };
    }
  }

  let plannerAttempt = await runPlanner(plannerUserPrompt);
  let plannerValidation = validatePlannerQueryPlan(plannerAttempt.parsed);

  let repairCount = 0;
  while (!plannerValidation.ok && repairCount < 2) {
    repairCount++;
    const repairPrompt = `REPAIR MODE.
Validation errors:
${plannerValidation.errors.join("\n")}

Original user message:
${normalizedMessage}

Allowlist + ConceptMap:
${schemaPrompt}

Invalid JSON:
${plannerAttempt.raw}

Return ONLY corrected JSON that satisfies the schema and constraints.`;

    plannerAttempt = await runPlanner(repairPrompt);
    plannerValidation = validatePlannerQueryPlan(plannerAttempt.parsed);
  }

  if (!plannerValidation.ok) {
    const fallbackLanguage = language;
    return {
      plannerLanguage: fallbackLanguage,
      plannerStatus: "need_clarification",
      askUser:
        fallbackLanguage === "fr"
          ? "Pouvez-vous preciser votre demande (ville, type de projet, ou statut) ?"
          : "Could you clarify your request (city, project type, or status)?",
      executablePlan: {
      intent: { type: "lookup", table: "projects", columns: ["name"] },
      filters: [],
      limit: 10,
      sort: undefined,
      },
    };
  }

  const plannerPlan = plannerValidation.plan;
  const effectivePlannerPlan =
    plannerPlan.status !== "ok" && shouldAutoListProjects(normalizedMessage)
      ? {
          ...plannerPlan,
          status: "ok" as const,
          intent: "lookup" as const,
          select: ["PROJECT_NAME", "LOCATION_CITY", "PROJECT_STATUS"] as PlannerConcept[],
          filters: [],
          sort: [],
          aggregation: null,
          ask_user: null,
          notes: `${plannerPlan.notes} | Auto-resolved broad project listing.`,
        }
      : plannerPlan;

  const hintedSelection = applyConceptSelectionHints(
    {
      select: effectivePlannerPlan.select,
      notes: effectivePlannerPlan.notes,
    },
    normalizedMessage,
  );
  effectivePlannerPlan.select = hintedSelection.select;
  effectivePlannerPlan.notes = hintedSelection.notes;

  const tableMeta = allowlist.tables.get(effectivePlannerPlan.table.toLowerCase());
  if (!tableMeta) {
    throw new Error(`Planner selected unknown table "${effectivePlannerPlan.table}".`);
  }

  const resolveConcept = (concept: PlannerConcept): string | null => {
    const candidates = tableMeta.conceptMap[concept] || [];
    if (concept === "LOCATION_CITY" || concept === "LOCATION_TEXT") {
      const ranked = rankLocationColumns(candidates);
      return ranked.length > 0 ? ranked[0] : null;
    }
    return candidates.length > 0 ? candidates[0] : null;
  };

  const resolveConceptForSelect = (concept: PlannerConcept): string[] => {
    const candidates = tableMeta.conceptMap[concept] || [];
    if (candidates.length === 0) return [];

    // For range-like business concepts (price/surface), include both min/max variants when present.
    if (concept === "PRICE" || concept === "SURFACE") {
      const ranged = candidates.filter((col) => /(min|max)/i.test(col));
      if (ranged.length > 0) {
        return Array.from(new Set([...ranged.slice(0, 2), candidates[0]]));
      }
    }

    return [candidates[0]];
  };

  const resolvedSelect = Array.from(
    new Set(
      effectivePlannerPlan.select.flatMap((concept) => resolveConceptForSelect(concept)),
    ),
  );

  const resolvedFilters = effectivePlannerPlan.filters
    .map((filter) => {
      const column = resolveConcept(filter.concept);
      if (!column) return null;
      const mappedOperator =
        filter.operator === "="
          ? "eq"
          : filter.operator === "!="
            ? "neq"
            : filter.operator === "IN"
              ? "in"
              : "contains";
      return {
        field: column,
        operator: mappedOperator as QueryPlan["filters"][number]["operator"],
        value: filter.value,
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  const resolvedSort = effectivePlannerPlan.sort
    .map((item) => {
      const column = resolveConcept(item.concept);
      if (!column) return null;
      return {
        field: column,
        direction: item.direction,
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  const aggregationColumn =
    effectivePlannerPlan.aggregation && effectivePlannerPlan.aggregation.concept
      ? resolveConcept(effectivePlannerPlan.aggregation.concept)
      : null;

  const executablePlan: QueryPlan = {
    intent: {
      type: effectivePlannerPlan.intent,
      table: effectivePlannerPlan.table,
      columns: resolvedSelect.length > 0 ? resolvedSelect : undefined,
      aggregation:
        effectivePlannerPlan.intent === "aggregate" && effectivePlannerPlan.aggregation && aggregationColumn
          ? {
              func: effectivePlannerPlan.aggregation.func,
              column: aggregationColumn,
            }
          : undefined,
    },
    filters: resolvedFilters,
    limit: Math.max(1, Math.min(50, effectivePlannerPlan.limit)),
    sort: resolvedSort.length > 0 ? resolvedSort[0] : undefined,
  };

  const shapeValidation = validateQueryPlan(executablePlan);
  if (!shapeValidation.ok) {
    throw new Error(`Invalid QueryPlan JSON: ${shapeValidation.errors.join(" ")}`);
  }

  const allowlistValidation = validateAgainstAllowlist(shapeValidation.plan, allowlist);
  if (!allowlistValidation.ok) {
    throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);
  }

  return {
    plannerLanguage: plannerPlan.language,
    plannerStatus: effectivePlannerPlan.status,
    askUser: effectivePlannerPlan.ask_user,
    executablePlan: applyIntentAndOperatorHints(shapeValidation.plan, normalizedMessage),
  };
}

async function generateAnswer(
  language: ChatLanguage,
  message: string,
  plan: QueryPlan,
  rows: Record<string, unknown>[],
): Promise<string> {
  const presentationRows = enrichRowsWithPriceRange(sanitizeRowsForResponder(rows, message), message);
  const response = await queryAI(
    `You are a client-facing real-estate sales agent.
You help clients as a professional real estate advisor.
Reply in ${language === "fr" ? "French" : "English"}.
Use only the provided query results and never invent missing facts.
Keep a warm, concise, professional tone.
Sound like a helpful agent, not a technical support bot.
Do not mention SQL, query plans, allowlists, backend, internal validation, or system errors.
Do not expose technical identifiers (id/uuid/coordinates) unless the user explicitly asked for them.
When there are results:
- Prefer project name, address/city, and description when available.
- If pricing columns include min/max variants (e.g. minprice/maxprice), always present price as a range:
  "between {min} and {max}" (or use provided price_range field if present).
- Present results as short, natural paragraphs (not bullet lists).
- Keep phrasing commercial and client-friendly.
- If there are many rows, provide a compact summary plus 2-5 representative examples.
When there are no results:
- Say this politely.
- Suggest one concrete rephrasing in the same language.
Return JSON only with shape: {"answer":"string"}.`,
    `User question:
${message}

QueryPlan:
${JSON.stringify(plan)}

Rows:
${JSON.stringify(presentationRows)}`,
    true,
  );

  try {
    const parsed = JSON.parse(response) as { answer?: string };
    if (typeof parsed.answer === "string" && parsed.answer.trim().length > 0) {
      if (rows.length > 0 && looksLikeNoResultsAnswer(parsed.answer)) {
        return buildDeterministicRowsAnswer(language, presentationRows);
      }
      return parsed.answer;
    }
  } catch {
    // Fall through.
  }

  if (rows.length > 0) {
    return buildDeterministicRowsAnswer(language, presentationRows);
  }

  return language === "fr"
    ? "Je n'ai pas pu formater une reponse claire, mais les resultats sont fournis."
    : "I could not format a clear answer, but the query results are provided.";
}

export async function chat(message: string, requestedLanguage?: unknown, sessionId: string = "default"): Promise<ChatResponse> {
  const existingState = conversationStore.get(sessionId);
  const resolvedLanguage = await resolveChatLanguage(message, requestedLanguage, existingState);
  const state = getConversationState(sessionId, resolvedLanguage);
  state.language = resolvedLanguage;
  const extractedCity = extractLikelyLocationTerm(message);
  if (extractedCity) state.city = extractedCity;
  const extractedBudget = extractBudget(message);
  if (extractedBudget) state.budget = extractedBudget;
  const extractedBedrooms = extractBedrooms(message);
  if (extractedBedrooms) state.bedrooms = extractedBedrooms;
  const extractedFeatures = extractFeatureTags(message);
  if (extractedFeatures.length > 0) {
    state.features = Array.from(new Set([...state.features, ...extractedFeatures]));
  }

  if (detectVisitIntent(message) || (state.selectedProject && detectAffirmativeMessage(message))) {
    state.wantsVisit = true;
    state.mode = "lead_capture";
  }

  if (state.mode === "lead_capture") {
    if (!state.lead.name) {
      const name = extractLeadName(message);
      if (name) {
        state.lead.name = name;
      }
    }
    if (!state.lead.phone) {
      const phone = extractLeadPhone(message);
      if (phone) {
        state.lead.phone = phone;
      }
    }
    if (!state.lead.email) {
      const email = extractLeadEmail(message);
      if (email) {
        state.lead.email = email;
      }
    }

    if (!state.lead.name || !state.lead.phone) {
      const missing: Array<"name" | "phone" | "email" | "availability"> = [];
      if (!state.lead.name) missing.push("name");
      if (!state.lead.phone) missing.push("phone");
      missing.push("email", "availability");
      return {
        language: resolvedLanguage,
        status: "need_clarification",
        conversation_status: "lead_capture",
        required_fields: missing,
        answer:
          resolvedLanguage === "fr"
            ? "Parfait. Pour organiser la visite, merci de partager votre nom complet et votre numero de telephone."
            : "Great. To schedule the visit, please share your full name and phone number.",
        queryPlan: EMPTY_PLAN,
        results: [],
      };
    }

    if (!state.lead.email) {
      return {
        language: resolvedLanguage,
        status: "need_clarification",
        conversation_status: "lead_capture",
        required_fields: ["email", "availability"],
        answer:
          resolvedLanguage === "fr"
            ? "Merci. Pouvez-vous aussi partager votre adresse email ?"
            : "Thank you. Could you also share your email address?",
        queryPlan: EMPTY_PLAN,
        results: [],
      };
    }

    if (!state.lead.availability) {
      const availability = extractLeadAvailability(message);
      if (availability) {
        if (!isAllowedAppointmentSlot(availability.date)) {
          return {
            language: resolvedLanguage,
            status: "need_clarification",
            conversation_status: "lead_capture",
            required_fields: ["availability"],
            answer:
              resolvedLanguage === "fr"
                ? "Les rendez-vous sont disponibles du lundi au vendredi, de 9h a 12h et de 14h a 18h. Merci de proposer un autre creneau."
                : "Appointments are available Monday to Friday, from 9:00 to 12:00 and 14:00 to 18:00. Please propose another slot.",
            queryPlan: EMPTY_PLAN,
            results: [],
          };
        }
        state.lead.availability = availability.raw;
        state.lead.appointmentDateIso = availability.date.toISOString();
      } else {
        return {
          language: resolvedLanguage,
          status: "need_clarification",
          conversation_status: "lead_capture",
          required_fields: ["availability"],
          answer:
            resolvedLanguage === "fr"
              ? "Parfait. Merci d'indiquer un creneau de visite (jour et heure) entre lundi-vendredi, 9h-12h ou 14h-18h."
              : "Perfect. Please provide a visit slot (day and time) between Monday-Friday, 9:00-12:00 or 14:00-18:00.",
          queryPlan: EMPTY_PLAN,
          results: [],
        };
      }
    }

    state.mode = "normal";
    try {
      await persistAppointment(state);
    } catch (error) {
      console.error("[appointments] Failed to persist appointment", error);
      return {
        language: resolvedLanguage,
        status: "need_clarification",
        conversation_status: "lead_capture",
        required_fields: ["availability"],
        answer:
          resolvedLanguage === "fr"
            ? "Je n'ai pas pu enregistrer le rendez-vous pour le moment. Pouvez-vous confirmer votre disponibilite (jour et heure) ?"
            : "I could not save the appointment right now. Could you confirm your availability (day and time)?",
        queryPlan: EMPTY_PLAN,
        results: [],
      };
    }
    return {
      language: resolvedLanguage,
      status: "ok",
      conversation_status: "appointment_ready",
      answer:
        resolvedLanguage === "fr"
          ? `Merci ${state.lead.name}. Nous avons bien note votre demande${state.selectedProject ? ` pour ${state.selectedProject}` : ""}. Un agent immobilier vous appellera au ${state.lead.phone} et vous confirmera le rendez-vous (email: ${state.lead.email}, disponibilite: ${state.lead.availability}).`
          : `Thank you ${state.lead.name}. We have recorded your request${state.selectedProject ? ` for ${state.selectedProject}` : ""}. A real estate agent will call you at ${state.lead.phone} to confirm the appointment (email: ${state.lead.email}, availability: ${state.lead.availability}).`,
      queryPlan: EMPTY_PLAN,
      results: [],
    };
  }

  if (detectBroadHousingIntent(message)) {
    const nextField = requiredQualificationField(state);
    if (nextField === "city") {
      return {
        language: resolvedLanguage,
        status: "need_clarification",
        conversation_status: "qualifying",
        answer: buildQualificationQuestion(resolvedLanguage, nextField),
        suggestions: buildQualificationChips(resolvedLanguage, nextField),
        queryPlan: EMPTY_PLAN,
        results: [],
      };
    }
  }

  const allowlist = await loadSchemaAllowlist();
  const broadInventoryExecution = await runBroadInventorySearch(message);
  const presetExecution = broadInventoryExecution ?? (await runPresetQuery(message));
  if (presetExecution) {
    const rows = dedupeRows(presetExecution.rows);
    const inferredSourceTable =
      firstExistingValue(rows[0] || {}, ["sourcetable"])?.toLowerCase() || presetExecution.plan.intent.table;
    const projectCards = mapRowsToProjectCards(enrichRowsWithPriceRange(rows, message), inferredSourceTable);
    if (!state.selectedProject && projectCards.length > 0) {
      state.selectedProject = projectCards[0].name;
      state.selectedProjectTable = projectCards[0].source_table;
      if (typeof projectCards[0].id === "string") {
        if (projectCards[0].source_table === "immeubles") {
          state.selectedImmeubleId = projectCards[0].id;
        } else {
          state.selectedProjectId = projectCards[0].id;
        }
      }
      if (typeof projectCards[0].project_id === "string") {
        state.selectedProjectId = projectCards[0].project_id;
      }
    }
    const answer = await generateAnswer(resolvedLanguage, message, presetExecution.plan, rows);
    const suggestions =
      rows.length > 8
        ? await buildChatSuggestions({
            allowlist,
            message,
            language: resolvedLanguage,
            tableKey: presetExecution.plan.intent.table,
          })
        : undefined;

    const { followUpQuestion, conversationStatus } = buildPostResultFollowUp(
      resolvedLanguage,
      message,
      state,
      rows.length > 0,
    );

    return {
      language: resolvedLanguage,
      status: "ok",
      answer,
      follow_up_question: followUpQuestion,
      suggestions,
      conversation_status: conversationStatus,
      projects: projectCards,
      queryPlan: presetExecution.plan,
      results: rows,
    };
  }

  const forcedPlan = buildForcedProjectsAddressPlan(message, allowlist);
  const planning = forcedPlan
    ? {
        executablePlan: forcedPlan,
        plannerLanguage: resolvedLanguage,
        plannerStatus: "ok" as PlannerStatus,
        askUser: null,
      }
    : await generateQueryPlan(message, resolvedLanguage, allowlist);
  const plan = planning.executablePlan;
  const broadRequest = shouldAutoListProjects(message);

  if (planning.plannerStatus !== "ok" || broadRequest) {
    const lang = resolvedLanguage;
    const defaultCityQuestion =
      lang === "fr" ? "D'accord, vous cherchez des projets dans quelle ville ?" : "Sure, which city are you interested in?";
    const fallbackQuestion = lang === "fr" ? "Pouvez-vous reformuler votre demande ?" : "Could you rephrase your request?";
    const followUp = broadRequest ? defaultCityQuestion : fallbackQuestion;
    const suggestions = await buildChatSuggestions({
      allowlist,
      message,
      language: lang,
      tableKey: plan.intent.table,
    });
    return {
      language: lang,
      status: broadRequest ? "need_clarification" : planning.plannerStatus,
      answer: followUp,
      follow_up_question: followUp,
      suggestions,
      conversation_status: "qualifying",
      queryPlan: plan,
      results: [],
    };
  }
  const allowlistValidation = validateAgainstAllowlist(plan, allowlist);

  if (!allowlistValidation.ok) {
    throw new Error(`QueryPlan rejected by allowlist: ${allowlistValidation.errors.join(" ")}`);
  }

  const normalizedPlan = normalizeSemanticFilters(plan, allowlistValidation.table.columns, message);
  const denoisedPlan = removeNoisyNameFilters(normalizedPlan, allowlistValidation.table.columns);
  const deblockedPlan = removeAmbiguousAvailabilityFilters(denoisedPlan, allowlistValidation.table.columns);
  const locationEnforcedPlan = enforceLocationFilterFromMessage(deblockedPlan, allowlistValidation.table.columns, message);
  const directLocationPlan = applyDirectProjectLocationOverride(
    locationEnforcedPlan,
    allowlistValidation.table.columns,
    message,
  );
  const adjustedPlan = applyClientFriendlyProjection(directLocationPlan, allowlistValidation.table.columns, message);
  let executedPlan = adjustedPlan;
  let { sql, params } = compileQueryPlan(executedPlan, allowlistValidation.table);
  let dbResult = await query(sql, params);
  logSql("main", sql, params, dbResult.rows.length);
  let rows = dedupeRows(dbResult.rows as Record<string, unknown>[]);

  if (rows.length === 0) {
    const relaxed = buildRelaxedLocationPlan(executedPlan, allowlistValidation.table.columns, message);
    if (relaxed) {
      executedPlan = relaxed;
      const retry = compileQueryPlan(executedPlan, allowlistValidation.table);
      dbResult = await query(retry.sql, retry.params);
      logSql("relaxed", retry.sql, retry.params, dbResult.rows.length);
      rows = dedupeRows(dbResult.rows as Record<string, unknown>[]);

      // Secondary fallback: try alternate textual location columns if still empty.
      if (rows.length === 0) {
        const firstLocationFilter = executedPlan.filters.find((f) => hasLocationLikeColumn(f.field));
        if (firstLocationFilter) {
          const alternatives = getAlternativeLocationColumns(allowlistValidation.table.columns, firstLocationFilter.field);
          for (const altCol of alternatives) {
            const altPlan: QueryPlan = {
              ...executedPlan,
              filters: executedPlan.filters.map((f) =>
                f === firstLocationFilter ? { ...f, field: altCol, operator: "contains" } : f,
              ),
            };
            const altQuery = compileQueryPlan(altPlan, allowlistValidation.table);
            const altResult = await query(altQuery.sql, altQuery.params);
            logSql(`alt-column:${altCol}`, altQuery.sql, altQuery.params, altResult.rows.length);
            const altRows = dedupeRows(altResult.rows as Record<string, unknown>[]);
            if (altRows.length > 0) {
              executedPlan = altPlan;
              rows = altRows;
              break;
            }
          }
        }
      }
    }
  }

  if (rows.length === 0) {
    const cityFallbackPlans = buildCityFallbackPlans(message, allowlist);
    for (const candidatePlan of cityFallbackPlans) {
      const candidateAllowlist = validateAgainstAllowlist(candidatePlan, allowlist);
      if (!candidateAllowlist.ok) continue;
      const candidateQuery = compileQueryPlan(candidatePlan, candidateAllowlist.table);
      const candidateResult = await query(candidateQuery.sql, candidateQuery.params);
      logSql(
        `city-fallback:${candidatePlan.intent.table}:${candidatePlan.filters[0]?.field || "none"}`,
        candidateQuery.sql,
        candidateQuery.params,
        candidateResult.rows.length,
      );
      const candidateRows = dedupeRows(candidateResult.rows as Record<string, unknown>[]);
      if (candidateRows.length > 0) {
        executedPlan = candidatePlan;
        rows = candidateRows;
        break;
      }
    }
  }

  if (rows.length === 0) {
    const rescued = await runProjectCityRescueLookup(message, allowlist);
    if (rescued) {
      executedPlan = rescued.plan;
      rows = rescued.rows;
    }
  }

  const answer = await generateAnswer(resolvedLanguage, message, executedPlan, rows);
  const projectCards = mapRowsToProjectCards(enrichRowsWithPriceRange(rows, message), executedPlan.intent.table);
  if (!state.selectedProject && projectCards.length > 0) {
    state.selectedProject = projectCards[0].name;
    state.selectedProjectTable = projectCards[0].source_table;
    if (typeof projectCards[0].id === "string") {
      if (projectCards[0].source_table === "immeubles") {
        state.selectedImmeubleId = projectCards[0].id;
      } else {
        state.selectedProjectId = projectCards[0].id;
      }
    }
    if (typeof projectCards[0].project_id === "string") {
      state.selectedProjectId = projectCards[0].project_id;
    }
  }
  const { followUpQuestion, conversationStatus } = buildPostResultFollowUp(
    resolvedLanguage,
    message,
    state,
    rows.length > 0,
  );
  const suggestions =
    rows.length > 8
      ? await buildChatSuggestions({
          allowlist,
          message,
          language: resolvedLanguage,
          tableKey: executedPlan.intent.table,
        })
      : undefined;

  return {
    language: resolvedLanguage,
    status: "ok",
    answer,
    follow_up_question: followUpQuestion,
    suggestions,
    conversation_status: conversationStatus,
    projects: projectCards,
    queryPlan: executedPlan,
    results: rows,
  };
}
