import { useMemo, useState } from "react";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"

type Language = "en" | "fr";

type ChatResponse = {
  language: Language;
  answer: string;
  queryPlan: unknown;
  results: Array<Record<string, unknown>>;
};

const copy = {
  en: {
    title: "SQL Server Chat",
    subtitle: "Ask questions about your database in natural language.",
    inputLabel: "Message",
    placeholder: "e.g. Show the top 10 customers in Paris",
    ask: "Ask",
    asking: "Asking...",
    answer: "Answer",
    results: "Results",
    noRows: "No rows returned.",
    language: "Language",
    en: "English",
    fr: "French",
    error: "Chat request failed.",
  },
  fr: {
    title: "Chat SQL Server",
    subtitle: "Posez des questions sur votre base de donnees en langage naturel.",
    inputLabel: "Message",
    placeholder: "ex. Affiche les 10 meilleurs clients a Paris",
    ask: "Envoyer",
    asking: "Traitement...",
    answer: "Reponse",
    results: "Resultats",
    noRows: "Aucune ligne retournee.",
    language: "Langue",
    en: "Anglais",
    fr: "Francais",
    error: "Echec de la requete chat.",
  },
} as const;

function App() {
  const [language, setLanguage] = useState<Language>("en");
  const [query, setQuery] = useState("");
  const [answer, setAnswer] = useState<string | null>(null);
  const [rows, setRows] = useState<Array<Record<string, unknown>>>([]);
  const [error, setError] = useState<string | null>(null);
  const [isQuerying, setIsQuerying] = useState(false);

  const text = useMemo(() => copy[language], [language]);
  const visibleColumns = useMemo(() => {
    if (rows.length === 0) return [];
    const all = Object.keys(rows[0]);
    const blocked = [/^id$/i, /_id$/i, /uuid/i, /guid/i, /lat/i, /lng/i, /lon/i, /coord/i, /^location$/i];
    const filtered = all.filter((key) => !blocked.some((pattern) => pattern.test(key)));
    return filtered.length > 0 ? filtered : all;
  }, [rows]);

  const handleQuery = async () => {
    if (!query) return;

    setError(null);
    setIsQuerying(true);
    try {
      const response = await fetch("http://localhost:3000/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ message: query, language }),
      });

      if (!response.ok) throw new Error(text.error);

      const data = (await response.json()) as ChatResponse;
      setAnswer(data.answer);
      setRows(data.results || []);
    } catch (err) {
      console.error("Chat error:", err);
      setError(text.error);
    } finally {
      setIsQuerying(false);
    }
  };

  return (
    <div className="min-h-screen bg-background p-4">
      <div className="max-w-4xl mx-auto">
        <Card>
          <CardHeader>
            <CardTitle>{text.title}</CardTitle>
            <p className="text-sm text-muted-foreground">{text.subtitle}</p>
          </CardHeader>
          <CardContent>
            <div className="mb-4 flex items-center gap-2">
              <Label>{text.language}</Label>
              <Button
                variant={language === "en" ? "default" : "outline"}
                onClick={() => setLanguage("en")}
              >
                {text.en}
              </Button>
              <Button
                variant={language === "fr" ? "default" : "outline"}
                onClick={() => setLanguage("fr")}
              >
                {text.fr}
              </Button>
            </div>
            <Label htmlFor="query">{text.inputLabel}</Label>
            <div className="flex gap-2">
              <input
                type="text"
                id="query"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="flex-1 px-3 py-2 border rounded-md"
                placeholder={text.placeholder}
              />
              <Button onClick={handleQuery} disabled={isQuerying}>
                {isQuerying ? text.asking : text.ask}
              </Button>
            </div>

            {isQuerying && (
              <div className="mt-4">
                <Skeleton className="h-20 w-full" />
              </div>
            )}

            {error && !isQuerying && <p className="mt-4 text-sm text-red-600">{error}</p>}

            {answer && !isQuerying && (
              <div className="mt-4 space-y-3">
                <div className="p-4 bg-muted rounded-md">
                  <p className="text-xs text-muted-foreground mb-1">{text.answer}</p>
                  <p>{answer}</p>
                </div>
                <div className="p-4 border rounded-md overflow-x-auto">
                  <p className="text-xs text-muted-foreground mb-2">{text.results}</p>
                  {rows.length === 0 ? (
                    <p>{text.noRows}</p>
                  ) : (
                    <table className="w-full text-sm">
                      <thead>
                        <tr>
                          {visibleColumns.map((key) => (
                            <th className="text-left p-2 border-b" key={key}>
                              {key}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map((row, index) => (
                          <tr key={index}>
                            {visibleColumns.map((key) => (
                              <td className="p-2 border-b" key={key}>
                                {String(row[key])}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            )}
          </CardContent>
          <CardFooter />
        </Card>
      </div>
    </div>
  )
}

export default App
