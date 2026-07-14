import * as React from 'react';
import { Drawer as Primitive } from 'vaul';
import styles from './styles/Drawer.module.css';
import { cn } from '@/ui/utils';

namespace Drawer {
  export namespace Root {
    export type Props = React.ComponentProps<typeof Primitive.Root>;
  }

  export function Root({ shouldScaleBackground = true, ...props }: Drawer.Root.Props) {
    return <Primitive.Root shouldScaleBackground={shouldScaleBackground} {...props} />;
  }

  export const Trigger = Primitive.Trigger;
  export const Portal = Primitive.Portal;
  export const Close = Primitive.Close;

  export namespace Overlay {
    export type Props = React.ComponentPropsWithoutRef<typeof Primitive.Overlay> & {
      className?: string;
    };
  }

  export function Overlay({ className, ...props }: Drawer.Overlay.Props) {
    return <Primitive.Overlay className={cn(styles.overlay, className)} {...props} />;
  }

  export namespace Content {
    export type Props = React.ComponentPropsWithoutRef<typeof Primitive.Content> & {
      className?: string;
      children?: React.ReactNode;
    };
  }

  export function Content({ className, children, ...props }: Drawer.Content.Props) {
    return (
      <Portal>
        <Overlay />
        <Primitive.Content className={cn(styles.content, className)} {...props}>
          <div className={styles.handle} />
          {children}
        </Primitive.Content>
      </Portal>
    );
  }

  export namespace Header {
    export type Props = React.HTMLAttributes<HTMLDivElement>;
  }

  export function Header({ className, ...props }: Drawer.Header.Props) {
    return <div className={cn(styles.header, className)} {...props} />;
  }

  export namespace Footer {
    export type Props = React.HTMLAttributes<HTMLDivElement>;
  }

  export function Footer({ className, ...props }: Drawer.Footer.Props) {
    return <div className={cn(styles.footer, className)} {...props} />;
  }

  export namespace Title {
    export type Props = React.ComponentPropsWithoutRef<typeof Primitive.Title> & {
      className?: string;
    };
  }

  export function Title(props: Drawer.Title.Props) {
    return <Primitive.Title className={cn(styles.title, props.className)} {...props} />;
  }

  export namespace Description {
    export type Props = React.ComponentPropsWithoutRef<typeof Primitive.Description> & {
      className?: string;
    };
  }

  export function Description(props: Drawer.Description.Props) {
    return <Primitive.Description className={cn(styles.description, props.className)} {...props} />;
  }
}

export default Drawer;
