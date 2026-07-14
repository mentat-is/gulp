import { SetState } from "@/class/API";
import { GulpDataset } from "@/class/Info";
import { Toggle } from "@/ui/Toggle";
import { Icon } from "@/ui/Icon";
import { capitalize } from "lodash";
import { ChangeEvent, useEffect } from "react";
import { Stack } from "@/ui/Stack";
import { Input } from "@/ui/Input";
import { Label } from "@/ui/Label";

export namespace CustomParameters {
  export type Location = 'query';

  export type Type = 'int' | 'str' | 'bool' | 'dict' | 'list';

  export interface Interface {
    name: string;
    type: Type;
    desc: string;
    location: Location;
    required: boolean;
    default_value?: any
    values?: string[]
    // internal
    invalid?: boolean;
  }

  export namespace Editor {
    export interface Props {
      plugin?: GulpDataset.PluginList.Interface;
      customParameters: Record<string, any>;
      setCustomParameters: SetState<Record<string, any>>;
    }
  }
  export function Editor({ plugin, customParameters, setCustomParameters }: CustomParameters.Editor.Props) {
    if (!plugin || !customParameters) {
      return null;
    }

    useEffect(() => {
      plugin.custom_parameters.forEach(parameter => {
        if (!customParameters[parameter.name]) {
          customParameters[parameter.name] = parameter.default_value ?? null;
        }
      })

      setCustomParameters(customParameters);
    }, [plugin]);

    const customParameterInputChangeHandlerConstructor =
      (name: string) => (event: ChangeEvent<HTMLInputElement>) => {
        const { value: raw } = event.target

        const type = plugin.custom_parameters.find(
          (p) => p.name === name,
        )?.type

        const value = !type
          ? raw
          : type === 'list'
            ? raw.split(',').map((v) => v.trim())
            : type === 'int'
              ? Number(raw) || 0
              : type === 'dict'
                ? raw
                : type === 'bool'
                  ? Boolean(raw)
                  : raw

        setCustomParameters({
          ...customParameters,
          [name]: value
        })
      }

    return (
      <Stack dir='column' ai='stretch'>
        {Object.entries(customParameters).map(([k, value], i, arr) => {
          const param = plugin.custom_parameters.find(c => c.name === k)
          if (!param) return null

          if (param.type === 'bool') {
            return (
              <Stack key={k} dir='column' gap={6} ai='flex-start' style={{ width: '100%' }}>
                <Label value={param.desc} />
                <Toggle
                  onCheckedChange={v => setCustomParameters(c => ({ ...c, [k]: v }))}
                  checked={value}
                  option={['Disabled', 'Enabled']}
                />
              </Stack>
            )
          }

          return (
            <Input
              key={k}
              placeholder={`${k} value should be in ${param.type} format`}
              onChange={customParameterInputChangeHandlerConstructor(k)}
              value={Array.isArray(value) ? value.join(', ') : value}
              variant="highlighted"
              label={capitalize(param.desc)}
              icon='Status'
            />
          )
        })}
      </Stack>
    )
  }
}
