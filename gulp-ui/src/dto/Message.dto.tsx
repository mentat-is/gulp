export interface Message {
  status: number
  message: string
  img?: string
}

export type MessageTypes = 'ok' | 'warning' | 'error'
