import { FormEvent, useEffect, useMemo, useState } from "react";
import { History, Home, Languages, MessageCircle, Moon, Plus, SendHorizonal, Sun } from "lucide-react";
import { Button } from "@/components/ui/button";

type Language = "en" | "fr";
type Role = "user" | "assistant";
type Theme = "light" | "dark";

interface ChatMessage {
  id: string;
  role: Role;
  content: string;
  suggestions?: SuggestionChip[];
  projects?: ProjectCard[];
}

interface ChatSession {
  id: string;
  title: string;
  createdAt: string;
  updatedAt: string;
  messages: ChatMessage[];
}

interface ChatApiResponse {
  language: Language;
  answer: string;
  follow_up_question?: string;
  suggestions?: SuggestionChip[];
  projects?: ProjectCard[];
  conversation_status?: "normal" | "qualifying" | "lead_capture" | "appointment_ready";
  required_fields?: Array<"name" | "phone" | "availability">;
}

interface SuggestionChip {
  label: string;
  payload: string;
  type?: string;
}

interface ProjectCard {
  name: string;
  city?: string;
  description?: string;
  price_range?: string;
  images: string[];
}

const STORAGE_KEY = "homebot-chat-sessions-v1";
const THEME_KEY = "homebot-theme-v1";
const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3000";

const content = {
  en: {
    appName: "HomeBot",
    subtitle: "Your real estate assistant",
    heroTitle: "How can I help you today?",
    heroSubtitle: "Ask me anything about houses, projects, and listings.",
    placeholder: "Ask about houses, projects...",
    historyTitle: "History",
    newChat: "New chat",
    noHistory: "No saved chats yet.",
    suggestions: [
      "Show me available projects in Casablanca",
      "Show me projects currently in progress",
      "Tell me about 3-bedroom apartments",
      "Show me the latest listings",
    ],
    failed: "I couldn't process that request. Please try again.",
  },
  fr: {
    appName: "HomeBot",
    subtitle: "Votre assistant immobilier",
    heroTitle: "Comment puis-je vous aider aujourd'hui ?",
    heroSubtitle: "Posez vos questions sur les maisons, projets et annonces.",
    placeholder: "Posez une question sur les biens, projets...",
    historyTitle: "Historique",
    newChat: "Nouveau chat",
    noHistory: "Aucun chat enregistre.",
    suggestions: [
      "Montre-moi les projets disponibles a Casablanca",
      "Montre-moi les projets en cours",
      "Parle-moi des appartements 3 chambres",
      "Montre-moi les dernieres annonces",
    ],
    failed: "Je n'ai pas pu traiter votre demande. Veuillez reessayer.",
  },
} as const;

function makeSession(): ChatSession {
  const now = new Date().toISOString();
  const id = `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  return {
    id,
    title: "New chat",
    createdAt: now,
    updatedAt: now,
    messages: [],
  };
}

function getSessionTitle(messages: ChatMessage[]): string {
  const firstUserMessage = messages.find((msg) => msg.role === "user");
  if (!firstUserMessage) return "New chat";
  const title = firstUserMessage.content.trim();
  if (title.length <= 40) return title;
  return `${title.slice(0, 40)}...`;
}

function sortSessionsByUpdatedAt(sessions: ChatSession[]): ChatSession[] {
  return [...sessions].sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime());
}

function renderMessageContent(content: string) {
  return <p className="whitespace-pre-line">{content}</p>;
}


function App() {
  const [language, setLanguage] = useState<Language>("en");
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string>("");
  const [theme, setTheme] = useState<Theme>("light");

  const locale = useMemo(() => content[language], [language]);

  useEffect(() => {
    const savedTheme = localStorage.getItem(THEME_KEY);
    if (savedTheme === "dark" || savedTheme === "light") {
      setTheme(savedTheme);
    }
  }, []);

  useEffect(() => {
    if (theme === "dark") {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
    localStorage.setItem(THEME_KEY, theme);
  }, [theme]);

  useEffect(() => {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      const initial = makeSession();
      setSessions([initial]);
      setActiveSessionId(initial.id);
      return;
    }

    try {
      const parsed = JSON.parse(raw) as ChatSession[];
      if (!Array.isArray(parsed) || parsed.length === 0) {
        const initial = makeSession();
        setSessions([initial]);
        setActiveSessionId(initial.id);
        return;
      }
      const sorted = sortSessionsByUpdatedAt(parsed);
      setSessions(sorted);
      setActiveSessionId(sorted[0].id);
    } catch {
      const initial = makeSession();
      setSessions([initial]);
      setActiveSessionId(initial.id);
    }
  }, []);

  useEffect(() => {
    if (sessions.length > 0) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(sessions));
    }
  }, [sessions]);

  const activeSession = useMemo(
    () => sessions.find((session) => session.id === activeSessionId) ?? null,
    [sessions, activeSessionId],
  );

  const updateActiveSession = (updater: (session: ChatSession) => ChatSession) => {
    setSessions((prev) =>
      sortSessionsByUpdatedAt(
        prev.map((session) => {
          if (session.id !== activeSessionId) return session;
          return updater(session);
        }),
      ),
    );
  };

  const createNewSession = () => {
    const session = makeSession();
    setSessions((prev) => [session, ...prev]);
    setActiveSessionId(session.id);
    setInput("");
  };

  const sendMessage = async (rawMessage: string) => {
    const message = rawMessage.trim();
    if (!message || sending || !activeSession) return;

    const userMessage: ChatMessage = {
      id: `${Date.now()}-user`,
      role: "user",
      content: message,
    };

    updateActiveSession((session) => {
      const nextMessages = [...session.messages, userMessage];
      return {
        ...session,
        title: getSessionTitle(nextMessages),
        messages: nextMessages,
        updatedAt: new Date().toISOString(),
      };
    });

    setInput("");
    setSending(true);

    try {
      const response = await fetch(`${API_URL}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message, language, session_id: activeSessionId }),
      });

      if (!response.ok) throw new Error("chat_request_failed");

      const data = (await response.json()) as ChatApiResponse;
      const assistantMessages: ChatMessage[] = [
        {
          id: `${Date.now()}-assistant`,
          role: "assistant",
          content: data.answer,
          suggestions: data.suggestions?.slice(0, 8),
          projects: data.projects,
        },
      ];

      if (
        data.follow_up_question &&
        data.follow_up_question.trim().toLowerCase() !== data.answer.trim().toLowerCase()
      ) {
        assistantMessages.push({
          id: `${Date.now()}-assistant-follow-up`,
          role: "assistant",
          content: data.follow_up_question,
          suggestions: data.suggestions?.slice(0, 8),
        });
      }

      updateActiveSession((session) => ({
        ...session,
        messages: [...session.messages, ...assistantMessages],
        updatedAt: new Date().toISOString(),
      }));
    } catch {
      updateActiveSession((session) => ({
        ...session,
        messages: [
          ...session.messages,
          {
            id: `${Date.now()}-assistant-error`,
            role: "assistant",
            content: locale.failed,
          },
        ],
        updatedAt: new Date().toISOString(),
      }));
    } finally {
      setSending(false);
    }
  };

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    await sendMessage(input);
  };

  const messages = activeSession?.messages ?? [];
  const isDark = theme === "dark";

  return (
    <div className={`flex h-screen flex-col overflow-hidden ${isDark ? "bg-slate-950 text-slate-100" : "bg-[#f4f5f4] text-slate-800"}`}>
      <header className={`h-20 shrink-0 border-b backdrop-blur-sm ${isDark ? "border-slate-800 bg-slate-900/90" : "border-slate-200 bg-white/85"}`}>
        <div className="mx-auto flex h-full w-full max-w-7xl items-center justify-between px-6">
          <button type="button" className="flex items-center gap-4" onClick={createNewSession}>
            <div className={`grid h-12 w-12 place-items-center rounded-2xl text-emerald-500 ${isDark ? "bg-emerald-900/40" : "bg-emerald-50"}`}>
              <Home className="h-6 w-6" />
            </div>
            <div className="text-left">
              <p className="text-2xl font-bold leading-none">{locale.appName}</p>
              <p className={`text-sm ${isDark ? "text-slate-400" : "text-slate-500"}`}>{locale.subtitle}</p>
            </div>
          </button>

          <div className={`flex items-center gap-2 rounded-full border p-1 ${isDark ? "border-slate-700 bg-slate-900" : "border-slate-200 bg-white"}`}>
            <Button
              size="sm"
              variant="ghost"
              className={`rounded-full ${isDark ? "text-amber-300 hover:bg-slate-800" : "text-slate-600 hover:bg-slate-100"}`}
              onClick={() => setTheme(isDark ? "light" : "dark")}
            >
              {isDark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </Button>
            <Languages className={`ml-1 h-4 w-4 ${isDark ? "text-slate-300" : "text-slate-500"}`} />
            <Button
              size="sm"
              variant={language === "en" ? "default" : "ghost"}
              className={language === "en" ? "rounded-full bg-emerald-600 hover:bg-emerald-700" : `rounded-full ${isDark ? "text-slate-200 hover:bg-slate-800" : ""}`}
              onClick={() => setLanguage("en")}
            >
              EN
            </Button>
            <Button
              size="sm"
              variant={language === "fr" ? "default" : "ghost"}
              className={language === "fr" ? "rounded-full bg-emerald-600 hover:bg-emerald-700" : `rounded-full ${isDark ? "text-slate-200 hover:bg-slate-800" : ""}`}
              onClick={() => setLanguage("fr")}
            >
              FR
            </Button>
          </div>
        </div>
      </header>

      <main className="mx-auto grid h-full min-h-0 w-full max-w-7xl grid-cols-1 gap-6 px-6 py-6 lg:grid-cols-[320px_1fr]">
        <aside className={`flex min-h-0 flex-col rounded-2xl border p-4 ${isDark ? "border-slate-800 bg-slate-900" : "border-slate-200 bg-white"}`}>
          <div className="mb-4 flex items-center justify-between">
            <div className={`flex items-center gap-2 ${isDark ? "text-slate-300" : "text-slate-600"}`}>
              <History className="h-4 w-4" />
              <span className="font-semibold">{locale.historyTitle}</span>
            </div>
            <Button
              size="sm"
              variant="outline"
              className={`rounded-full border-emerald-300 text-emerald-600 hover:bg-emerald-100/60 ${isDark ? "bg-slate-900" : ""}`}
              onClick={createNewSession}
            >
              <Plus className="mr-1 h-4 w-4" />
              {locale.newChat}
            </Button>
          </div>

          <div className="flex-1 space-y-2 overflow-y-auto pr-1">
            {sessions.length === 0 ? (
              <p className={`text-sm ${isDark ? "text-slate-400" : "text-slate-500"}`}>{locale.noHistory}</p>
            ) : (
              sortSessionsByUpdatedAt(sessions).map((session) => (
                <button
                  type="button"
                  key={session.id}
                  onClick={() => setActiveSessionId(session.id)}
                  className={`w-full rounded-xl px-3 py-2 text-left text-sm transition ${
                    session.id === activeSessionId
                      ? isDark ? "bg-emerald-900/40 text-emerald-200" : "bg-emerald-100 text-emerald-800"
                      : isDark ? "bg-slate-800 text-slate-200 hover:bg-slate-700" : "bg-slate-50 text-slate-700 hover:bg-slate-100"
                  }`}
                >
                  <div className="truncate font-medium">{session.title}</div>
                  <div className={`text-xs ${isDark ? "text-slate-400" : "text-slate-500"}`}>{new Date(session.updatedAt).toLocaleString()}</div>
                </button>
              ))
            )}
          </div>
        </aside>

        <section className="flex min-h-0 flex-col overflow-hidden">
          <div className="flex-1 overflow-y-auto px-1">
            {messages.length === 0 ? (
              <section className="mx-auto mt-10 flex w-full max-w-2xl flex-col items-center gap-6 text-center">
              <div className={`grid h-20 w-20 place-items-center rounded-3xl text-emerald-500 ${isDark ? "bg-emerald-900/40" : "bg-emerald-100"}`}>
                <Home className="h-10 w-10" />
              </div>
              <h1 className={`text-5xl font-bold tracking-tight ${isDark ? "text-slate-100" : "text-slate-800"}`}>{locale.heroTitle}</h1>
              <p className={`text-xl ${isDark ? "text-slate-400" : "text-slate-500"}`}>{locale.heroSubtitle}</p>

              <div className="mt-4 w-full space-y-3">
                {locale.suggestions.map((suggestion) => (
                  <button
                    key={suggestion}
                    type="button"
                    onClick={() => sendMessage(suggestion)}
                    className={`flex w-full items-center gap-3 rounded-2xl border px-5 py-4 text-left text-2xl shadow-sm transition ${
                      isDark
                        ? "border-slate-700 bg-slate-900 text-slate-200 hover:border-emerald-600 hover:bg-slate-800"
                        : "border-slate-200 bg-white text-slate-700 hover:border-emerald-200 hover:bg-emerald-50"
                    }`}
                  >
                    <MessageCircle className={`h-6 w-6 ${isDark ? "text-slate-500" : "text-slate-400"}`} />
                    <span>{suggestion}</span>
                  </button>
                ))}
              </div>
              </section>
            ) : (
              <section className="mx-auto flex w-full max-w-3xl flex-col gap-5 pb-6">
                {messages.map((message) => (
                  <div key={message.id} className={`flex ${message.role === "user" ? "justify-end" : "justify-start"}`}>
                    <div className="max-w-[85%]">
                      <div
                        className={`rounded-2xl px-5 py-4 text-lg leading-relaxed shadow-sm ${
                          message.role === "user"
                            ? "bg-emerald-600 text-white"
                            : isDark
                              ? "bg-slate-900 text-slate-100"
                              : "bg-white text-slate-800"
                        }`}
                      >
                        {renderMessageContent(message.content)}
                      </div>
                      {message.role === "assistant" && message.suggestions && message.suggestions.length > 0 && (
                        <div className="mt-3 flex flex-wrap gap-2">
                          {message.suggestions.slice(0, 8).map((chip, idx) => (
                            <button
                              key={`${message.id}-chip-${idx}`}
                              type="button"
                              disabled={sending}
                              onClick={() => sendMessage(chip.payload)}
                              className={`rounded-full border px-4 py-2 text-sm font-medium transition disabled:opacity-60 ${
                                isDark
                                  ? "border-emerald-700 bg-emerald-900/30 text-emerald-200 hover:bg-emerald-900/50"
                                  : "border-emerald-200 bg-emerald-50 text-emerald-800 hover:bg-emerald-100"
                              }`}
                            >
                              {chip.label}
                            </button>
                          ))}
                        </div>
                      )}
                      {message.role === "assistant" && message.projects && message.projects.length > 0 && (
                        <div className="mt-4 grid gap-3 sm:grid-cols-2">
                          {message.projects.slice(0, 6).map((project, idx) => (
                            <div
                              key={`${message.id}-project-${idx}`}
                              className={`overflow-hidden rounded-2xl border ${
                                isDark ? "border-slate-700 bg-slate-800" : "border-slate-200 bg-slate-50"
                              }`}
                            >
                              {project.images[0] ? (
                                <img
                                  src={project.images[0]}
                                  alt={project.name}
                                  className="h-36 w-full object-cover"
                                  loading="lazy"
                                />
                              ) : null}
                              <div className="space-y-1 p-3 text-sm">
                                <div className="font-semibold">{project.name}</div>
                                {project.city ? (
                                  <div className={isDark ? "text-slate-300" : "text-slate-600"}>{project.city}</div>
                                ) : null}
                                {project.price_range ? (
                                  <div className="text-emerald-600 dark:text-emerald-300">{project.price_range}</div>
                                ) : null}
                                {project.description ? (
                                  <div className={isDark ? "text-slate-400" : "text-slate-600"}>
                                    {project.description}
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </section>
            )}
          </div>

          <form
            onSubmit={onSubmit}
            className={`shrink-0 border-t backdrop-blur-sm ${isDark ? "border-slate-800 bg-slate-900/90" : "border-slate-200 bg-white/95"}`}
          >
            <div className="mx-auto flex w-full max-w-5xl items-center gap-3 px-6 py-4">
              <input
                type="text"
                value={input}
                onChange={(event) => setInput(event.target.value)}
                placeholder={locale.placeholder}
                className={`h-14 flex-1 rounded-2xl border px-5 text-lg outline-none ring-0 transition focus:border-emerald-400 ${
                  isDark
                    ? "border-slate-700 bg-slate-800 text-slate-100 placeholder:text-slate-400"
                    : "border-slate-200 bg-white text-slate-800"
                }`}
              />
              <Button
                type="submit"
                disabled={sending || input.trim().length === 0}
                className="h-14 rounded-2xl bg-emerald-600 px-6 text-lg hover:bg-emerald-700"
              >
                <SendHorizonal className="h-5 w-5" />
              </Button>
            </div>
          </form>
        </section>
      </main>
    </div>
  );
}

export default App;
