import { NextRequest, NextResponse } from "next/server";
import {
  startGenieConversation,
  continueGenieConversation,
  pollGenieMessage,
  getGenieQueryResult,
} from "@/lib/queries/genie-client";

export const dynamic = "force-dynamic";

const GENIE_SPACE_ID = process.env.GENIE_SPACE_ID ?? "";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { action, question, conversationId, messageId, spaceId } = body as {
      action: "ask" | "continue" | "poll" | "query-result";
      question?: string;
      conversationId?: string;
      messageId?: string;
      spaceId?: string;
    };

    const sid = spaceId || GENIE_SPACE_ID;
    if (!sid) {
      return NextResponse.json(
        { error: "GENIE_SPACE_ID not configured. Create a Genie Space and set the env var." },
        { status: 400 }
      );
    }

    switch (action) {
      case "ask": {
        if (!question) return NextResponse.json({ error: "question required" }, { status: 400 });
        const result = await startGenieConversation(sid, question);
        return NextResponse.json(result);
      }
      case "continue": {
        if (!conversationId || !question) {
          return NextResponse.json({ error: "conversationId and question required" }, { status: 400 });
        }
        const result = await continueGenieConversation(sid, conversationId, question);
        return NextResponse.json(result);
      }
      case "poll": {
        if (!conversationId || !messageId) {
          return NextResponse.json({ error: "conversationId and messageId required" }, { status: 400 });
        }
        const msg = await pollGenieMessage(sid, conversationId, messageId);
        return NextResponse.json(msg);
      }
      case "query-result": {
        if (!conversationId || !messageId) {
          return NextResponse.json({ error: "conversationId and messageId required" }, { status: 400 });
        }
        const qr = await getGenieQueryResult(sid, conversationId, messageId);
        return NextResponse.json(qr);
      }
      default:
        return NextResponse.json({ error: "Unknown action" }, { status: 400 });
    }
  } catch (err) {
    return NextResponse.json(
      { error: err instanceof Error ? err.message : String(err) },
      { status: 500 }
    );
  }
}
