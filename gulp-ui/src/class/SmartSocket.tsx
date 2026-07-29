import { Logger } from "@/dto/Logger.class"
import { Internal } from '@/entities/addon/Internal'
import { Doc } from "@/entities/Doc"
import { Group } from "@/entities/Group"
import { Operation } from "@/entities/Operation"
import { Request } from "@/entities/Request"
import { User } from "@/entities/User"
import { Icon } from "@/ui/Icon"
import { EventEmitter } from 'events'

export namespace SmartSocket {
  export type Condition<T = any> = (data: Message.Entity<T>) => boolean
  export type Handler<T = any> = (data: Message.Entity<T>) => void

  interface Conditional<T = any> {
    id: string
    condition: Condition<T>
    handler: Handler<T>
    once?: boolean
  }

  export namespace Message {
    export enum Type {
      WS_ERROR = "ws_error",
      WS_CONNECTED = "ws_connected",
      STATS_CREATE = "stats_create",
      STATS_UPDATE = "stats_update",
      COLLAB_CREATE = "collab_create",
      COLLAB_UPDATE = "collab_update",
      USER_LOGIN = "user_login",
      USER_LOGOUT = "user_logout",
      DOCUMENTS_CHUNK = "docs_chunk",
      COLLAB_DELETE = "collab_delete",
      INGEST_SOURCE_DONE = "ingest_source_done",
      ENRICH_DONE = "enrich_done",
      TAG_DONE = "tag_done",
      REBASE_DONE = "rebase_done",
      CLIENT_DATA = "client_data",
      SOURCE_FIELDS_CHUNK = "source_fields_chunk",
      AI_ASSISTANT_DONE = "ai_assistant_done",
      AI_ASSISTANT_STREAM = "ai_assistant_stream",
      AI_ASSISTANT_ERROR = "ai_assistant_error",
    }

    export interface Entity<T = any> {
      '@timestamp': number
      type: Message.Type
      private: boolean
      payload: {
        obj: T
        last?: boolean
        docs?: Doc.Type[]
        [key: string]: any
      }
      ws_id: string
      user_id: User.Id
      req_id: Request.Id
      operation_id?: Operation.Id
      'gulp.operation_id'?: Operation.Id
    }
  }

  export class Class extends EventEmitter {
    public static instance: SmartSocket.Class

    private ws!: WebSocket
    private readonly conditional: Map<SmartSocket.Message.Type, Conditional[]> = new Map()
    private counter = 0

    private token!: string
    private ws_id!: string

    private reconnectAttempts = 0
    private reconnectTimer: any = null
    private manuallyClosed = false

    private readonly reconnectBaseDelay = 500
    private readonly reconnectMaxDelay = 10_000

    constructor(ws_id: string) {
      super()

      if (SmartSocket.Class.instance) {
        return SmartSocket.Class.instance
      }

      this.token = Internal.Settings.token
      this.ws_id = ws_id

      this.connect()

      SmartSocket.Class.instance = this
    }

    private connect() {
      this.ws = new WebSocket(Internal.Settings.server + "/ws")
      this.forwarding()
    }

    private forwarding() {
      this.ws.onopen = (event) => {
        this.reconnectAttempts = 0

        if (this.reconnectTimer) {
          clearTimeout(this.reconnectTimer)
          this.reconnectTimer = null
        }

        this.send({
          token: this.token,
          ws_id: this.ws_id,
        })

        this.once(SmartSocket.Message.Type.WS_CONNECTED, () =>
          Logger.log(`WebSocket has been initialized`, SmartSocket.Class, {
            icon: <Icon name='Rss' />
          })
        )

        this.emit('open', event)
      }

      this.ws.onclose = (event) => {
        this.emit('close', event)

        if (!this.manuallyClosed) {
          this.scheduleReconnect()
        }
      }

      this.ws.onerror = (event) => {
        this.emit('error', event)

        /**
         * Usually the browser will fire `close` after `error`.
         * Do not reconnect directly here, otherwise we may schedule twice.
         */
      }

      this.ws.onmessage = (event) => {
        let message: any

        try {
          message = JSON.parse(event.data)
        } catch (err) {
          this.emit(SmartSocket.Message.Type.WS_ERROR, {
            error: err,
            raw: event.data,
          })
          return
        }

        this.emit(message.type, message)
        this.handle(message.type as SmartSocket.Message.Type, message)
      }
    }

    private scheduleReconnect() {
      if (this.reconnectTimer) return

      const delay = Math.min(
        this.reconnectBaseDelay * 2 ** this.reconnectAttempts,
        this.reconnectMaxDelay
      )

      this.reconnectAttempts += 1

      Logger.log(
        `WebSocket disconnected. Reconnecting in ${delay}ms`,
        SmartSocket.Class
      )

      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null

        try {
          this.connect()
        } catch (err) {
          Logger.log(
            `Failed to reconnect websocket: ${(err as Error).message}`,
            SmartSocket.Class
          )

          this.scheduleReconnect()
        }
      }, delay)
    }

    private handle(event: SmartSocket.Message.Type, data: any) {
      const listeners = this.conditional.get(event) || []
      const remove: string[] = []

      for (const listener of listeners) {
        try {
          if (listener.condition(data)) {
            listener.handler(data)
            if (listener.once) remove.push(listener.id)
          }
        } catch (err) {
          Logger.log(
            `Handler error for ${event}: ${(err as Error).message}`,
            SmartSocket.Class
          )
        }
      }

      for (const id of remove) this.remove(event, id)
    }

    private remove(event: SmartSocket.Message.Type, id: string) {
      const listeners = this.conditional.get(event) || []
      const filtered = listeners.filter(l => l.id !== id)

      if (filtered.length === 0) {
        this.conditional.delete(event)
      } else {
        this.conditional.set(event, filtered)
      }
    }

    con<T = any>(
      event: SmartSocket.Message.Type,
      condition: Condition<T>,
      handler: Handler<T>
    ): string {
      const id = `listener_${++this.counter}`
      const listeners = this.conditional.get(event) ?? []
      listeners.push({ id, condition, handler, once: false })
      this.conditional.set(event, listeners)
      return id
    }

    conce<T = any>(
      event: SmartSocket.Message.Type,
      condition: Condition<T>,
      handler: Handler<T>
    ): string {
      const id = `listener_${++this.counter}`
      const listeners = this.conditional.get(event) ?? []
      listeners.push({ id, condition, handler, once: true })
      this.conditional.set(event, listeners)
      return id
    }

    coff(event: SmartSocket.Message.Type, listenerId: string): void {
      this.remove(event, listenerId)
    }

    coffAll(event: SmartSocket.Message.Type): void {
      this.conditional.delete(event)
    }

    coffWhenCondition<T = any>(
      event: SmartSocket.Message.Type,
      condition: Condition<T>
    ): void {
      const listeners = this.conditional.get(event) || []
      const filtered = listeners.filter(l => l.condition !== condition)

      if (filtered.length === 0) {
        this.conditional.delete(event)
      } else {
        this.conditional.set(event, filtered)
      }
    }

    wait<T = any>(
      event: SmartSocket.Message.Type,
      condition: Condition<T>,
      timeout?: number
    ): Promise<Message.Entity<T>> {
      return new Promise((resolve, reject) => {
        let timeoutId: any = null

        const listenerId = this.conce<T>(event, condition, (data) => {
          if (timeoutId) clearTimeout(timeoutId)
          resolve(data as Message.Entity<T>)
        })

        if (timeout) {
          timeoutId = setTimeout(() => {
            this.coff(event, listenerId)
            reject(new Error(`Timeout waiting for ${event} event`))
          }, timeout)
        }
      })
    }

    send(data: object): void {
      try {
        if (this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify(data))
        } else {
          Logger.log('WebSocket not open, cannot send message', SmartSocket.Class)
        }
      } catch (err) {
        Logger.log(
          `Failed to send websocket message: ${(err as Error).message}`,
          SmartSocket.Class
        )
      }
    }

    close(code?: number, reason?: string): void {
      this.manuallyClosed = true

      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer)
        this.reconnectTimer = null
      }

      try {
        this.ws.close(code, reason)
      } catch (err) {
        Logger.log(
          `Failed to close websocket: ${(err as Error).message}`,
          SmartSocket.Class
        )
      }
    }

    /**
     * Permanently releases this realm's socket singleton and all of its listeners.
     *
     * Detached assistant routes use this when their authenticated main context
     * becomes inactive or their private connection identity changes. Clearing
     * the singleton is important because a later authenticated bootstrap must
     * create a live socket instead of reusing the previously closed instance.
     */
    destroy(): void {
      this.close()
      this.conditional.clear()
      this.removeAllListeners()

      if (SmartSocket.Class.instance === this) {
        Reflect.deleteProperty(SmartSocket.Class, 'instance')
      }
    }

    reconnect(): void {
      this.manuallyClosed = false

      try {
        this.ws.close()
      } catch {
        // ignore
      }

      this.scheduleReconnect()
    }
  }
}
