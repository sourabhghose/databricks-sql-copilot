import { getConfig } from "@/lib/config";
import { fetchWithTimeout, TIMEOUTS } from "@/lib/dbx/fetch-with-timeout";

/**
 * Always use the app service principal's all-apis token for Genie calls.
 * The OBO user token's `dashboards.genie` scope is not accepted by the
 * Genie Conversation API (it requires the literal `genie` scope which
 * cannot be requested through the Apps scope picker). The SP token with
 * `all-apis` works reliably — the SP just needs `Can Run` on the space.
 */
async function getSpBearerToken(): Promise<string> {
  const config = getConfig();
  if (config.auth.mode === "pat") return config.auth.token;

  if (config.auth.mode !== "oauth") {
    throw new Error("Genie client requires OAuth (SP) or PAT credentials.");
  }

  const tokenUrl = `https://${config.serverHostname}/oidc/v1/token`;
  const body = new URLSearchParams({ grant_type: "client_credentials", scope: "all-apis" });
  const credentials = btoa(`${config.auth.clientId}:${config.auth.clientSecret}`);
  const response = await fetchWithTimeout(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded", Authorization: `Basic ${credentials}` },
    body: body.toString(),
  }, { timeoutMs: TIMEOUTS.AUTH });
  if (!response.ok) throw new Error(`Genie SP OAuth failed: ${response.status}`);
  const data = (await response.json()) as { access_token: string };
  return data.access_token;
}

export interface GenieMessage {
  id: string;
  content: string;
  role: "user" | "assistant";
  sql?: string;
  sqlResult?: unknown[][];
  sqlColumns?: string[];
  status: "COMPLETED" | "EXECUTING_QUERY" | "FILTERING_RESULTS" | "ASKING_AI" | "FAILED";
}

export async function startGenieConversation(
  spaceId: string,
  question: string,
): Promise<{ conversationId: string; messageId: string }> {
  const config = getConfig();
  const token = await getSpBearerToken();
  const url = `https://${config.serverHostname}/api/2.0/genie/spaces/${spaceId}/start-conversation`;
  const res = await fetchWithTimeout(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ content: question }),
    cache: "no-store",
  }, { timeoutMs: 60000 });
  if (!res.ok) throw new Error(`Genie start failed: ${res.status} ${await res.text()}`);
  const data = (await res.json()) as { conversation_id: string; message_id: string };
  return { conversationId: data.conversation_id, messageId: data.message_id };
}

export async function continueGenieConversation(
  spaceId: string,
  conversationId: string,
  question: string,
): Promise<{ messageId: string }> {
  const config = getConfig();
  const token = await getSpBearerToken();
  const url = `https://${config.serverHostname}/api/2.0/genie/spaces/${spaceId}/conversations/${conversationId}/messages`;
  const res = await fetchWithTimeout(url, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ content: question }),
    cache: "no-store",
  }, { timeoutMs: 60000 });
  if (!res.ok) throw new Error(`Genie continue failed: ${res.status} ${await res.text()}`);
  const data = (await res.json()) as { id: string };
  return { messageId: data.id };
}

export async function pollGenieMessage(
  spaceId: string,
  conversationId: string,
  messageId: string,
): Promise<GenieMessage> {
  const config = getConfig();
  const token = await getSpBearerToken();
  const url = `https://${config.serverHostname}/api/2.0/genie/spaces/${spaceId}/conversations/${conversationId}/messages/${messageId}`;
  const res = await fetchWithTimeout(url, {
    method: "GET",
    headers: { Authorization: `Bearer ${token}` },
    cache: "no-store",
  }, { timeoutMs: 30000 });
  if (!res.ok) throw new Error(`Genie poll failed: ${res.status}`);
  const data = (await res.json()) as {
    id: string;
    status: string;
    attachments?: Array<{
      text?: { content: string };
      query?: { query: string; description: string };
    }>;
  };

  let content = "";
  let sql: string | undefined;
  const attachments = data.attachments ?? [];
  for (const att of attachments) {
    if (att.text?.content) content += att.text.content + "\n";
    if (att.query?.query) sql = att.query.query;
  }

  return {
    id: data.id,
    content: content.trim() || "Thinking...",
    role: "assistant",
    sql,
    status: data.status as GenieMessage["status"],
  };
}

export async function getGenieQueryResult(
  spaceId: string,
  conversationId: string,
  messageId: string,
): Promise<{ columns: string[]; rows: unknown[][] }> {
  const config = getConfig();
  const token = await getSpBearerToken();
  const url = `https://${config.serverHostname}/api/2.0/genie/spaces/${spaceId}/conversations/${conversationId}/messages/${messageId}/query-result`;
  const res = await fetchWithTimeout(url, {
    method: "GET",
    headers: { Authorization: `Bearer ${token}` },
    cache: "no-store",
  }, { timeoutMs: 30000 });
  if (!res.ok) return { columns: [], rows: [] };
  const data = (await res.json()) as {
    statement_response?: {
      manifest?: { schema?: { columns?: Array<{ name: string }> } };
      result?: { data_array?: unknown[][] };
    };
  };
  const cols = data.statement_response?.manifest?.schema?.columns?.map((c) => c.name) ?? [];
  const rows = data.statement_response?.result?.data_array ?? [];
  return { columns: cols, rows };
}
