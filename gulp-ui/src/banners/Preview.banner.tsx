import { Banner as UIBanner } from '@/ui/Banner'
import { Table } from '@/components/Table'
import { useMemo, useState } from 'react';
import { cn } from '@/ui/utils';
import s from './styles/PreviewBanner.module.css'
import { Notification } from '@/ui/Notification';
import { Icon } from '@/ui/Icon';
import { Button } from '@/ui/Button';
import { Locale } from '@/locales';

export namespace Preview {
  export namespace Banner {
    export interface Props extends UIBanner.Props {
      total: number;
      values: Record<string, any>[]
    }
  }

  export function Banner({ total, values, children, ...props }: Banner.Props) {
    const { t } = Locale.use();
    const [isFullscreen, setIsFullscreen] = useState<boolean>(false);

    const OptionButton = useMemo(() => {
      return (
        <Button variant='tertiary' onClick={() => setIsFullscreen(v => !v)} icon={isFullscreen ? 'FullscreenClose' : 'Fullscreen'} />
      )
    }, [isFullscreen, setIsFullscreen]);

    return (
      <UIBanner className={cn(isFullscreen && s.fullscreen)} title={t('common.preview')} option={OptionButton} {...props}>
        <Table values={values} />
        <Preview.AmountNotification total={total} />
      </UIBanner>
    )
  }

  export namespace AmountNotification {
    export interface Props extends Notification.Props {
      total: number
    }
  }

  export function AmountNotification({ total, ...props }: AmountNotification.Props) {
    const { t } = Locale.use();
    if (typeof total !== 'number') {
      return null;
    }

    const isTooBig = total > 1_000_000;

    const variant: Notification.Variant = isTooBig ? 'warning' : 'success';
    const icon: Icon.Name = isTooBig ? 'Warning' : 'ShieldCheck';

    return useMemo(() => (
      <Notification icon={icon} variant={variant} {...props}>
        {t('preview.totalDocumentsPrefix')} <span>{total}</span>.{" "}
        <span>{isTooBig ? t('preview.queryUnsafe') : t('preview.queryFine')}</span>
      </Notification>
    ), [total, t]);
  }
}
