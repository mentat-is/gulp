import { Button } from '@/ui/Button'
import { Stack } from '@/ui/Stack'
import { useState, useRef, useEffect } from 'react'
import { Application } from '@/context/Application.context'
import { Doc } from '@/entities/Doc'
import { Operation } from '@/entities/Operation'
import { SmartSocket } from '@/class/SmartSocket'
import { Internal } from '@/entities/addon/Internal'
import { Markdown } from '@/ui/Markdown'
import { toast } from 'sonner'
import s from './styles/AIAssistantBanner.module.css'
import { cn } from '@/ui/utils'
import { GulpIndexedDB } from '@/class/IndexedDB'
import { copy, generateUUID } from '@/ui/utils'
import { WindowBridge } from '@/lib/WindowBridge'
import { Locale } from '@/locales'

// initialize indexed db and store if not already initialized
const chatDB = new GulpIndexedDB('gulp_DB', 'gulp_ai_assistant_history');

export namespace AIAssistant {

  export enum Role {
    USER = 'user',
    ASSISTANT = 'assistant'
  }

  export interface Message {
    content: string;
    role: Role;
  }

  /**
   * Represents a single request-response cycle in the AI chat.
   * Contains the ID of the request, the list of event IDs analyzed,
   * the user's prompt message, and the AI's generated response.
   */
  export interface AnalysisRequest {
    /** Unique identifier for the analysis request */
    id: string;
    eventIds: string[];
    userMessage: Message;
    aiMessage: Message;
  }

  export namespace Panel {
    export interface Props {
      message?: string;
    }
  }

  /**
   * Main component for the AI Assistant Chat Panel.
   * Handles checking indexedDB for history, sending analysis requests,
   * receiving streaming responses via WebSocket, and rendering the chat UI.
   */
  export function Panel({ message: initialMessage }: Panel.Props) {
    const { Info, app } = Application.use()
    const { t } = Locale.use()
    const [loading, setLoading] = useState<boolean>(false);
    // History of requests
    const [history, setHistory] = useState<AnalysisRequest[]>([])
    const [message, setMessage] = useState(initialMessage ?? '');
    const chatRef = useRef<HTMLDivElement>(null)
    const mounted = useRef(true)
    const operation = Operation.Entity.selected(app);

    const bridgeId = useRef(WindowBridge.generateId())
    const bridgeRef = useRef<ReturnType<typeof WindowBridge.create> | null>(null)

    useEffect(() => {
      const bridge = WindowBridge.create(bridgeId.current, (message) => {
        if (message.type === WindowBridge.MessageType.AI_HISTORY_UPDATED) {
          const payload = message.payload as { senderId?: string }
          if (payload.senderId === bridgeId.current) return;

          if (operation) {
            chatDB.GetConfiguration(operation.id).then((saved) => {
              if (mounted.current && saved) {
                setHistory(saved);
              }
            })
          }
        }
      });
      bridgeRef.current = bridge;
      return () => {
        bridge.destroy();
        bridgeRef.current = null;
      }
    }, [operation?.id]);

    useEffect(() => {
      mounted.current = true

      // If no operation is selected, we cannot load history context, so clear it.
      if (!operation) {
        // setHistory([]);
        return;
      }

      // Load chat history from IndexedDB for the current operation ID.
      if (operation) {
        chatDB.GetConfiguration(operation.id).then((saved) => {
          if (mounted.current) {
            if (saved) {
              setHistory(saved);
            }
            else {
              setHistory([])
            }
          }
        })
      }

      return () => {
        mounted.current = false
      }
    }, [operation?.id])

    useEffect(() => {
      if (!operation || history.length === 0) return;

      // Skip DB updates and broadcasts during streaming to prevent flickering and loops.
      // The final state will be synced when loading (streaming) finishes.
      if (loading) return;

      chatDB.UpdateConfiguration(history, operation.id).then(() => {
        bridgeRef.current?.send(WindowBridge.MessageType.AI_HISTORY_UPDATED, {
          senderId: bridgeId.current
        });
      });

      // Scroll to bottom only on new requests
      if (chatRef.current && loading) {
        chatRef.current.scrollTop = chatRef.current.scrollHeight
      }
    }, [history, operation?.id, loading])

    /**
     * Updates the content of the AI message for the most recent analysis request.
     * Used during streaming validation to append incoming chunks of text.
     * 
     * @param content - The full content string to update the message with.
     */
    const updateLastRequestAIMessage = (content: string) => {
      setHistory(prev => {
        const newHistory = [...prev]
        const lastIndex = newHistory.length - 1
        if (lastIndex >= 0) {
          const lastRequest = { ...newHistory[lastIndex] }
          lastRequest.aiMessage = { ...lastRequest.aiMessage, content }
          newHistory[lastIndex] = lastRequest
        }
        return newHistory
      })
    }

    /**
     * Sends the retrieved logs to the backend to get an AI hint/analysis.
     * Sets up WebSocket listeners to handle the streaming response.
     * 
     * @param logs - The list of log documents to be analyzed.
     * @param requestId - The ID of the current analysis request.
     */
    const getHint = async (logs: any[], requestId: string) => {
      if (!operation) return;

      setLoading(true);

      await api('/get_ai_hint', {
        method: 'POST',
        query: {
          token: Internal.Settings.token,
          ws_id: Info.app.general.ws_id,
          operation_id: operation.id
        },
        body: logs,
        raw: true
      }, ({ req_id }: { req_id: string }) => {
        let content = '';

        const stream = SmartSocket.Class.instance.con(SmartSocket.Message.Type.AI_ASSISTANT_STREAM, m => m.req_id === req_id, m => {
          if (!m.payload || !mounted.current) return;

          content += String(m.payload);
          updateLastRequestAIMessage(content)
        });

        const done = SmartSocket.Class.instance.conce(SmartSocket.Message.Type.AI_ASSISTANT_DONE, m => m.req_id === req_id, () => {
          if (mounted.current) setLoading(false);
          SmartSocket.Class.instance.coff(SmartSocket.Message.Type.AI_ASSISTANT_STREAM, stream);
          SmartSocket.Class.instance.coff(SmartSocket.Message.Type.AI_ASSISTANT_ERROR, error);

          if (chatRef.current) {
            chatRef.current.scrollTop = chatRef.current.scrollHeight
          }
        });

        const error = SmartSocket.Class.instance.conce(SmartSocket.Message.Type.AI_ASSISTANT_ERROR, m => m.req_id === req_id, m => {
          if (mounted.current) setLoading(false);
          const errorMsg = m.payload ? String(m.payload as any) : t('aiAssistant.responseError');
          updateLastRequestAIMessage(errorMsg)

          SmartSocket.Class.instance.coff(SmartSocket.Message.Type.AI_ASSISTANT_STREAM, stream);
          SmartSocket.Class.instance.coff(SmartSocket.Message.Type.AI_ASSISTANT_DONE, done);
          if (chatRef.current) {
            chatRef.current.scrollTop = chatRef.current.scrollHeight
          }
        });
      });
    }

    /**
     * Triggered when the user clicks 'Analyze Flagged Events'.
     * 1. Retrieves currently flagged event IDs.
     * 2. Checks if these exact events have already been analyzed to prevent duplicates.
     * 3. If duplicate, scrolls to the existing request.
     * 4. If new, creates a new request, updates state, and initiates the API call.
     */
    const analyze = async () => {
      const docIds = Doc.Entity.flag.getDocIds(Info.app, operation?.id);

      if (!docIds.length) {
        toast.info(t('aiAssistant.noFlaggedEvents'))
        return;
      }

      const sortedIds = [...docIds].sort().join(',');

      // Check for duplicates in history to avoid re-analyzing the same set of events.
      // We sort the IDs to ensure the comparison is order-independent.
      const existingRequest = history.find(req => {
        const reqIds = [...req.eventIds].sort().join(',');
        return reqIds === sortedIds;
      });

      if (existingRequest) {
        const element = document.getElementById(`request-${existingRequest.id}`);
        if (element) {
          element.scrollIntoView({ behavior: 'smooth', block: 'start' });
          // Briefly highlight or flash? (Optional)
          toast.info(t('aiAssistant.alreadyAnalyzed'));
        }
        return;
      }

      const requestId = generateUUID() as string;
      const newRequest: AnalysisRequest = {
        id: requestId,
        eventIds: docIds,
        userMessage: {
          content: t('aiAssistant.analyzeFlaggedCount', { count: docIds.length }),
          role: Role.USER
        },
        aiMessage: {
          content: '',
          role: Role.ASSISTANT
        }
      };

      setHistory(prev => [...prev, newRequest]);

      // Scroll to bottom immediately
      setTimeout(() => {
        if (chatRef.current) {
          chatRef.current.scrollTop = chatRef.current.scrollHeight
        }
      }, 0)

      const logs = await Info.query_gulp(docIds, ['event.original', '@timestamp', 'agent.type', 'gulp.source_id', 'gulp.context_id'], true)

      await getHint(logs.docs, requestId);
    }

    return (
      <Stack ai='stretch' jc='start' dir='column' className={cn(s.wrapper, s.wrapper_flex)}>
        <Stack ai='stretch' jc='start' dir='column' gap={8} className={s.chat} ref={chatRef}>
          {history.map((req) => (
            <Stack key={req.id} id={`request-${req.id}`} dir='column' gap={8} style={{ width: '100%' }}>
              <Stack className={cn(s.message, s.user)} dir='column' gap={4} ai='flex-start'>
                <span>{t('aiAssistant.you')}</span>
                {req.userMessage.content}
              </Stack>
              <Stack dir='column' ai='flex-start' className={cn(s.message, s.ai)} pos='relative'>
                <Button icon='Copy' variant='tertiary' className={s.copy} onClick={() => copy(req.aiMessage.content)} />
                <span>{t('aiAssistant.response')}</span>
                <Markdown value={req.aiMessage.content} />
              </Stack>
            </Stack>
          ))}
        </Stack>
        <Stack dir='row' ai='center' gap={8} className={s.actions}>
          <Button icon='Search' variant='secondary' onClick={analyze} disabled={loading} style={{ width: '100%' }}>
            {t('aiAssistant.analyzeFlagged')}
          </Button>
          <Button icon='Trash2' style={{ width: '100%' }} variant='secondary' onClick={() => {
            const confirmed = window.confirm(t('aiAssistant.deleteHistoryConfirm'));
            if (!confirmed) return;
            setHistory([]);
            if (operation) {
              chatDB.DeleteConfiguration(operation.id).then(() => {
                bridgeRef.current?.send(WindowBridge.MessageType.AI_HISTORY_UPDATED, {
                  senderId: bridgeId.current
                });
              });
            }
            toast.info(t('aiAssistant.historyDeleted'));
          }}>{t('aiAssistant.clearHistory')}</Button>
        </Stack>
      </Stack>
    )
  }
}
