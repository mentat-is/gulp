import * as React from 'react';

export namespace Stack {
  export interface Props extends React.HTMLAttributes<HTMLDivElement> {
    gap?: React.CSSProperties['gap'];
    dir?: React.CSSProperties['flexDirection'];
    pos?: React.CSSProperties['position'];
    jc?: React.CSSProperties['justifyContent'];
    ai?: React.CSSProperties['alignItems'];
    ac?: React.CSSProperties['alignContent'];
    flex?: React.CSSProperties['flex'] | boolean;
    noShrink?: boolean;
  }
}

const Stack = React.forwardRef<HTMLDivElement, Stack.Props>(
  ({
    flex = 'inherit',
    gap = 8,
    dir: flexDirection = 'row',
    pos: position = 'unset',
    style,
    jc: justifyContent = 'normal',
    ai: alignItems = 'center',
    noShrink,
    ...props
  }, ref) => {
    return (
      <div style={{
        ...style,
        display: 'flex',
        gap,
        flex: typeof flex === 'boolean' ? Number(flex) : flex,
        flexDirection,
        justifyContent,
        alignItems,
        position,
        flexShrink: noShrink ? 0 : 1
      }} ref={ref} {...props} />
    )
  }
)

Stack.displayName = 'Stack';

export { Stack };
