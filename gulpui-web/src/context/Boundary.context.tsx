import React from 'react';
import { Stack } from '@/ui/Stack';
import { Images } from '@/components/Images';
import { useState, useCallback, useEffect } from 'react';
import { copy } from '@/ui/utils';
import { Badge } from '@/ui/Badge';
import { Button } from '@/ui/Button';
import { Icon } from '@/ui/Icon';
import { Application } from '@/context/Application.context';
import s from './styles/Boundary.module.css';
import { Locale } from '@/locales';

export namespace Boundary {
  export type Props = {
    children: React.ReactNode
  };

  interface WithDescription {
    name?: string;
    message: string;
    stack?: string;
  };

  interface State {
    errors: WithDescription[];
    componentStack: string
  };

  export class Provider extends React.Component<Props, State> {
    static instance: Provider | null = null;
    banners: React.ReactNode[] = [];

    constructor(props: Props) {
      super(props);
      this.state = { errors: [], componentStack: '' };
    }

    static getDerivedStateFromError(error: Error) {
      return { errors: [{ name: error.name, message: error.message, stack: error.stack }] };
    }

    componentDidCatch(error: Error, info: React.ErrorInfo) {
      console.error('[UI Error]', error, info.componentStack);
      this.setState({ componentStack: info.componentStack ?? '' });
    }

    componentDidMount() {
      Provider.instance = this;
    }

    componentWillUnmount() {
      Provider.instance = null;
    }

    showError(error: Error) {
      this.setState((prev) => ({
        errors: [...prev.errors, { name: error.name, message: error.message, stack: error.stack }],
      }));
    }

    closeError = () => {
      this.setState(() => ({ errors: [] }));
    };

    spawnBannerFunc = (node: React.ReactNode) => {
      this.banners.push(node);
      this.forceUpdate();
    };

    render() {
      if (this.state.errors.length) {
        return (
          <Stack ai='center' jc='center' className={s.wrapper}>
            <Stack pos='relative' className={s.block}>
              <Images.LeftCap />
              <Images.MiddleLine />
              <Images.RightCap />
              <Panel errors={this.state.errors} onClose={this.closeError} />
              {this.banners.map((banner, index) => (
                <React.Fragment key={index}>{banner}</React.Fragment>
              ))}
            </Stack>
          </Stack>
        );
      }
      return this.props.children;
    };
  }

  export function Panel({ errors, onClose }: Panel.Props) {
    const { Info } = Application.use();
    const { t } = Locale.use();
    const [isOnline, setIsOnline] = useState(navigator.onLine);
    const [current, setCurrent] = useState(0);

    const handleSaveLog = useCallback(() => {
      const content = errors
        .map((e, index) => `Error #${index + 1}:\n${e.name ?? 'Error'}: ${e.message}\n${e.stack ?? ''}`)
        .join('\n\n');

      const blob = new Blob([content], { type: 'text/plain' });
      const url = URL.createObjectURL(blob);

      const a = document.createElement('a');
      a.href = url;
      a.download = `error-log-${Date.now()}.txt`;
      a.click();

      URL.revokeObjectURL(url);
    }, [errors]);

    useEffect(() => {
      const handleOnline = () => setIsOnline(true);
      const handleOffline = () => setIsOnline(false);

      window.addEventListener('online', handleOnline);
      window.addEventListener('offline', handleOffline);

      return () => {
        window.removeEventListener('online', handleOnline);
        window.addEventListener('offline', handleOffline);
      };
    }, []);

    const visibleError = errors[current];

    const handleCopy = () => copy(`${visibleError.message}\n${visibleError.stack ?? ''}`);

    return (
      <Stack ai='stretch' jc='start' dir='column' className={s.panel} gap={16}>
        <Stack ai='center' jc='space-between' dir='row'>
          <Stack style={{ color: 'var(--gray-alpha-1000)' }} ai='center' jc='center' dir='row' gap={'6px'} className={s.pagination}>
            <Badge variant='gray-subtle' disabled={current === 0} onClick={() => setCurrent(i => i - 1)} icon='ArrowLeft' />
            {current + 1} / {errors.length}
            <Badge variant='gray-subtle' disabled={current === errors.length - 1} onClick={() => setCurrent(i => i + 1)} icon='ArrowRight' />
          </Stack>
          <Stack ai='center' jc='center' dir='row' gap={'6px'}>
            <Badge variant='gray-subtle' mono value={t('boundary.version', { version: '1.0.0' })} />
          </Stack>
        </Stack>
        <Panel.Actions onCopy={handleCopy} />
        <Stack ai='stretch' flex jc='center' dir='column' gap={16} style={{ height: 0 }}>
          <span>{visibleError.message}</span>
          {Panel.HINT_KEYS.map((hint, idx) => <p key={hint}>{idx + 1}. {t(hint)}</p>)}
          <Stack ai='flex-start' jc='center' dir='column' className={s.stackContainer} flex>
            <Stack ai='center' jc='start' dir='row' gap={6} className={s.stack} flex={0}>
              <span style={{ color: 'var(--gray-alpha-1000)' }}>{t('boundary.callStack')}</span>
              <Badge variant='gray-subtle' size='sm' value={visibleError.stack ? `${visibleError.stack.split('\n').length}` : ''} />
            </Stack>
            <Stack dir='column' className={s.stackText}>
              {Panel.parseStack(visibleError.stack, t('boundary.project')).map((item, index) => (
                <Panel.Item
                  key={index}
                  {...item}
                />
              ))}
            </Stack>
          </Stack>
          <Stack jc='flex-end' flex={0} gap={12}>
            {isOnline ? (
              <Button variant='tertiary' onClick={() => window.open('https://github.com/mentat-is/gulpui-web/issues/new', '_blank')}>{t('boundary.report')}</Button>
            ) : <Button variant='tertiary' onClick={handleSaveLog}>{t('boundary.saveLog')}</Button>}
            <Button variant='secondary' icon='X' onClick={onClose}>{t('boundary.ignore')}</Button>
            <Button variant='default' icon='Save' onClick={async () => { Info.session_create({ name: `error ${new Date().toISOString()}`, icon: 'Bug', color: 'red' }); onClose?.() }}>{t('session.saveTitle')}</Button>
          </Stack>
        </Stack>
      </Stack>
    );
  }

  export namespace Panel {
    export interface Props {
      errors: WithDescription[];
      sessionAutosaver?: () => Promise<void>;
      onClose?: () => void;
    }

    export const HINT_KEYS = [
      'boundary.hintReactVersions',
      'boundary.hintRulesOfHooks',
      'boundary.hintMultipleReactCopies',
      'boundary.hintReportError'
    ] as const;

    export const parseStack = (stack = '', projectLabel: string) => {
      const result: Item.Props[] = [];

      stack.split('\n')
        .map(line => {
          const match = line.match(/\(?([^\s()]+):(\d+):(\d+)\)?/);
          if (!match) return null;

          const [, fullPath, lineStr, colStr] = match;
          const [, pathSuffix] = fullPath.split('/src/');

          result.push({
            projectPath: pathSuffix ? `${projectLabel}/src/${pathSuffix}` : fullPath,
            clientType: fullPath.includes('/app/') ? 'app-client' : 'unknown',
            filePath: `/src/${pathSuffix || ''}`,
            line: parseInt(lineStr, 100),
            column: parseInt(colStr, 10)
          });
        });

      return result;
    };

    export namespace Item {
      export interface Props {
        projectPath: string;
        clientType: string;
        filePath: string;
        line?: number;
        column?: number;
      }
    }

    export function Item({ projectPath, clientType, filePath, line, column }: Boundary.Panel.Item.Props) {
      const { t } = Locale.use();
      return (
        <Stack ai='start' jc='center' dir='column' className={s.stackItem} flex={0}>
          <Stack ai='start' jc='start' dir='row' gap={6} className={s.stackHeader}>
            {t('boundary.project')} {projectPath} [{clientType}]
          </Stack>
          <Stack className={s.stackPosition}>
            {filePath}{projectPath} {line && column ? `(${line}:${column})` : ''}
          </Stack>
        </Stack>
      );
    }

    export namespace Actions {
      export interface Props {
        onCopy?: () => void;
      }
    }

    export function Actions({ onCopy }: Actions.Props) {
      const { t } = Locale.use();
      return (
        <Stack ai='center' jc='space-between' className={s.action}>
          <Badge variant='red-subtle' value={t('boundary.runtimeError')} style={{ fontFamily: 'var(--font-mono)', borderRadius: '6px' }} />
          <Stack className={s.action_block}>
            <Button rounded variant='secondary' icon='Copy' onClick={onCopy} size='sm' shape='icon' />
            <Button variant='secondary' asChild rounded size='sm' shape='icon'>
              <a href=''>
                <Icon name='BookOpen' size={12} />
              </a>
            </Button>
          </Stack>
        </Stack>
      )
    }
  }
}
