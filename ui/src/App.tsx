import { FormEvent, useEffect, useMemo, useState } from "react";
import { History, Home, Languages, MessageCircle, Plus, SendHorizonal } from "lucide-react";
import { Button } from "@/components/ui/button";

type Language = "en" | "fr";
type Role = "user" | "assistant";

interface ChatMessage {
  id: string;
  role: Role;
  content: string;
  suggestions?: SuggestionChip[];
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
}

interface SuggestionChip {
  label: string;
  payload: string;
  type?: string;
}

const STORAGE_KEY = "homebot-chat-sessions-v1";
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
      "What projects are currently in progress?",
      "Tell me about 3-bedroom apartments",
      "What are the latest listings?",
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
      "Quels projets sont en cours actuellement ?",
      "Parle-moi des appartements 3 chambres",
      "Quelles sont les dernieres annonces ?",
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
  const lines = content.split("\n").map((line) => line.trim()).filter(Boolean);
  const bulletLines = lines.filter((line) => /^[-•]\s+/.test(line) || /^\d+\.\s+/.test(line));

  if (bulletLines.length >= 2) {
    return (
      <ul className="list-disc space-y-2 pl-6">
        {bulletLines.map((line, index) => (
          <li key={`${line}-${index}`}>{line.replace(/^[-•]\s+/, "").replace(/^\d+\.\s+/, "")}</li>
        ))}
      </ul>
    );
  }

  return <p className="whitespace-pre-line">{content}</p>;
}

function App() {
  const [language, setLanguage] = useState<Language>("en");
  const [input, setInput] = useState("");
  const [sending, setSending] = useState(false);
  const [sessions, setSessions] = useState<ChatSession[]>([]);
  const [activeSessionId, setActiveSessionId] = useState<string>("");

  const locale = useMemo(() => content[language], [language]);

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
        body: JSON.stringify({ message, language }),
      });

      if (!response.ok) throw new Error("chat_request_failed");

      const data = (await response.json()) as ChatApiResponse;
      const assistantMessages: ChatMessage[] = [
        {
          id: `${Date.now()}-assistant`,
          role: "assistant",
          content: data.answer,
          suggestions: data.suggestions?.slice(0, 8),
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

  return (
    <div className="min-h-screen bg-[#f4f5f4] text-slate-800">
      <header className="h-20 border-b border-slate-200 bg-white/85 backdrop-blur-sm">
        <div className="mx-auto flex h-full w-full max-w-7xl items-center justify-between px-6">
          <button type="button" className="flex items-center gap-4" onClick={createNewSession}>
            <div className="grid h-12 w-12 place-items-center rounded-2xl bg-emerald-50 text-emerald-600">
              <Home className="h-6 w-6" />
            </div>
            <div className="text-left">
              <p className="text-2xl font-bold leading-none">{locale.appName}</p>
              <p className="text-sm text-slate-500">{locale.subtitle}</p>
            </div>
          </button>

          <div className="flex items-center gap-2 rounded-full border border-slate-200 bg-white p-1">
            <Languages className="ml-2 h-4 w-4 text-slate-500" />
            <Button
              size="sm"
              variant={language === "en" ? "default" : "ghost"}
              className={language === "en" ? "rounded-full bg-emerald-600 hover:bg-emerald-700" : "rounded-full"}
              onClick={() => setLanguage("en")}
            >
              EN
            </Button>
            <Button
              size="sm"
              variant={language === "fr" ? "default" : "ghost"}
              className={language === "fr" ? "rounded-full bg-emerald-600 hover:bg-emerald-700" : "rounded-full"}
              onClick={() => setLanguage("fr")}
            >
              FR
            </Button>
          </div>
        </div>
      </header>

      <main className="mx-auto grid w-full max-w-7xl grid-cols-1 gap-6 px-6 pb-32 pt-8 lg:grid-cols-[280px_1fr]">
        <aside className="rounded-2xl border border-slate-200 bg-white p-4">
          <div className="mb-4 flex items-center justify-between">
            <div className="flex items-center gap-2 text-slate-600">
              <History className="h-4 w-4" />
              <span className="font-semibold">{locale.historyTitle}</span>
            </div>
            <Button
              size="sm"
              variant="outline"
              className="rounded-full border-emerald-200 text-emerald-700 hover:bg-emerald-50"
              onClick={createNewSession}
            >
              <Plus className="mr-1 h-4 w-4" />
              {locale.newChat}
            </Button>
          </div>

          <div className="space-y-2">
            {sessions.length === 0 ? (
              <p className="text-sm text-slate-500">{locale.noHistory}</p>
            ) : (
              sortSessionsByUpdatedAt(sessions).map((session) => (
                <button
                  type="button"
                  key={session.id}
                  onClick={() => setActiveSessionId(session.id)}
                  className={`w-full rounded-xl px-3 py-2 text-left text-sm transition ${
                    session.id === activeSessionId
                      ? "bg-emerald-100 text-emerald-800"
                      : "bg-slate-50 text-slate-700 hover:bg-slate-100"
                  }`}
                >
                  <div className="truncate font-medium">{session.title}</div>
                  <div className="text-xs text-slate-500">{new Date(session.updatedAt).toLocaleString()}</div>
                </button>
              ))
            )}
          </div>
        </aside>

        <section className="min-h-[60vh]">
          {messages.length === 0 ? (
            <section className="mx-auto mt-10 flex w-full max-w-2xl flex-col items-center gap-6 text-center">
              <div className="grid h-20 w-20 place-items-center rounded-3xl bg-emerald-100 text-emerald-600">
                <Home className="h-10 w-10" />
              </div>
              <h1 className="text-5xl font-bold tracking-tight text-slate-800">{locale.heroTitle}</h1>
              <p className="text-xl text-slate-500">{locale.heroSubtitle}</p>

              <div className="mt-4 w-full space-y-3">
                {locale.suggestions.map((suggestion) => (
                  <button
                    key={suggestion}
                    type="button"
                    onClick={() => sendMessage(suggestion)}
                    className="flex w-full items-center gap-3 rounded-2xl border border-slate-200 bg-white px-5 py-4 text-left text-2xl text-slate-700 shadow-sm transition hover:border-emerald-200 hover:bg-emerald-50"
                  >
                    <MessageCircle className="h-6 w-6 text-slate-400" />
                    <span>{suggestion}</span>
                  </button>
                ))}
              </div>
            </section>
          ) : (
            <section className="mx-auto flex w-full max-w-3xl flex-col gap-5">
              {messages.map((message) => (
                <div key={message.id} className={`flex ${message.role === "user" ? "justify-end" : "justify-start"}`}>
                  <div className="max-w-[85%]">
                    <div
                      className={`rounded-2xl px-5 py-4 text-lg leading-relaxed shadow-sm ${
                        message.role === "user" ? "bg-emerald-600 text-white" : "bg-white text-slate-800"
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
                            className="rounded-full border border-emerald-200 bg-emerald-50 px-4 py-2 text-sm font-medium text-emerald-800 transition hover:bg-emerald-100 disabled:opacity-60"
                          >
                            {chip.label}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </section>
          )}
        </section>
      </main>

      <form
        onSubmit={onSubmit}
        className="fixed bottom-0 left-0 right-0 border-t border-slate-200 bg-white/95 backdrop-blur-sm"
      >
        <div className="mx-auto flex w-full max-w-5xl items-center gap-3 px-6 py-4">
          <input
            type="text"
            value={input}
            onChange={(event) => setInput(event.target.value)}
            placeholder={locale.placeholder}
            className="h-14 flex-1 rounded-2xl border border-slate-200 bg-white px-5 text-lg outline-none ring-0 transition focus:border-emerald-300"
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
    </div>
  );
}

export default App;
