import { DisplayEventDialog } from '@/dialogs/Event.dialog'
import { DisplayGroupDialog } from '@/dialogs/Group.dialog'
import { toast, ToastT } from 'sonner'
import { Source } from '@/entities/Source'
import { Doc } from '@/entities/Doc'
import { Parser } from '@/entities/addon/Parser'

export type LogLevel = 'log' | 'warn' | 'error' | 'debug' | 'verbose' | 'fatal'

export interface LogHistory {
  level: LogLevel
  message: string
}

interface Options extends Omit<ToastT, 'id'> {
  toast?: 'success' | 'warning' | 'error'
}

type ExecutionContext = string | { name: string };

export class Logger {
  protected static messages: LogHistory[] = []

  static log(message: any, context: string | { name: string }, options?: Options) {
    Logger.store('log', message, typeof context === 'string' ? context : context?.name, options)
  }

  static error(message: any, context?: ExecutionContext, options?: Options) {
    Logger.store('error', message, context, options ? {
      ...options,
      toast: 'error'
    } : undefined);
  }

  static warn(message: any, context?: ExecutionContext, options?: Options) {
    Logger.store('debug', message, context, options ? {
      ...options,
      toast: 'warning'
    } : undefined);
  }

  static debug(message: any, context?: ExecutionContext, options?: Options) {
    Logger.store('debug', message, context, options)
  }

  static verbose(message: any, context?: ExecutionContext) {
    Logger.store('verbose', message, context)
  }

  static fatal(message: any, context?: ExecutionContext): never {
    Logger.store('fatal', message, context)
    throw new Error(message);
  }

  public static store(
    level: LogLevel,
    message: string,
    context?: ExecutionContext,
    options?: Options
  ) {
    Logger.messages.push({
      level,
      message: Logger.format(level, message, context),
    })

    if (!options)
      return;

    if (options.toast) {
      toast[options.toast](message, options);
    } else {
      toast(message, options);
    }
  }

  public static history = () => Logger.messages

  public static clear = () => (Logger.messages = [])

  public static push = (message: string) =>
    Logger.messages.push({ level: 'fatal', message })

  public static assert = <T extends any>(
    condition: T,
    message: string,
    context?: string,
  ): T => {
    if (!condition) {
      Logger.error(message, context)
    }
    return condition
  }

  private static preformat: Record<LogLevel, keyof typeof _Logger> = {
    log: 'green',
    warn: 'yellow',
    error: 'red',
    debug: 'magenta',
    verbose: 'cyan',
    fatal: 'white',
  }

  private static format(
    level: LogLevel,
    message: any,
    context?: ExecutionContext,
    trace?: string,
  ) {
    const timestamp = new Date().toISOString()
    const pid = `${typeof process !== 'undefined' ? ` ${process.pid}` : ''}`
    const contextInfo = context ? `[${Parser.array(context).join('.')}] ` : ''
    const traceInfo = trace ? ` ${trace}` : ''

    const log = (
      _Logger[Logger.preformat[level]] as (text?: string | null) => string
    )(
      `[${level.toUpperCase()}]${pid} - ${_Logger.white(timestamp)} ${level.toUpperCase().padStart(7, ' ')} ${_Logger.yellow(contextInfo)}${typeof message === 'object' ? JSON.stringify(message, (_, v) => (typeof v === 'bigint' ? v.toString() : v), 2) : message}${traceInfo}`,
    )

    return log
  }
}

export class LoggerHandler {
  public static canvasClick = (
    file: Source.Type,
    events: Doc.Type[],
    position: number,
  ) => {
    Logger.log(`Canvas has been clicked and file ${file.name}-${file.id} was trigerred. Position: ${position}`, LoggerHandler)

    if (!events.length) {
      return Logger.log(`No events on click position. Skipping...`, LoggerHandler)
    }

    if (events.length === 1) {
      return Logger.log(`One event on click position. Opening ${DisplayEventDialog.name} with ${events[0]._id}`, LoggerHandler)
    }

    Logger.log(`Events amount on click position: ${events.length}. Opening ${DisplayGroupDialog.name} with events ${JSON.stringify(events.map((e) => e._id), null, 2)}`, LoggerHandler)
  }
}

export class _Logger {
  public static red = (text?: string | null) => `\x1b[31m${text}\x1b[0m`
  public static green = (text?: string | null) => `\x1b[32m${text}\x1b[0m`
  public static yellow = (text?: string | null) => `\x1b[33m${text}\x1b[0m`
  public static blue = (text?: string | null) => `\x1b[34m${text}\x1b[0m`
  public static magenta = (text?: string | null) => `\x1b[35m${text}\x1b[0m`
  public static cyan = (text?: string | null) => `\x1b[36m${text}\x1b[0m`
  public static white = (text?: string | null) => `\x1b[37m${text}\x1b[0m`

  public static bold = (text: string | null) => `\x1b[1m${text}\x1b[0m`
  public static bold_red = (text?: string | null) =>
    `\x1b[31m\x1b[1m${text}\x1b[0m`
}
