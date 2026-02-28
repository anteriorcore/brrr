{ ... }:
{
  perSystem =
    { lib, config, ... }:
    let
      brrrScope = config.brrr.scope;
    in
    {
      devshells =
        let
          sharedCommands = [
            {
              name = "brrr-demo-deps";
              category = "demo";
              help = "Start all dependent services without any brrr workers / server";
              command = ''
                nix run .#deps
              '';
            }
            {
              name = "brrr-demo-full";
              category = "demo";
              help = "Launch a full demo locally";
              command = ''
                nix run .#demo
              '';
            }
          ];
          sharedEnvs = {
            AWS_ENDPOINT_URL = "http://localhost:8000";
            AWS_ACCESS_KEY_ID = "fake";
            AWS_SECRET_ACCESS_KEY = "fake";
            BRRR_TEST_REDIS_URL = "redis://localhost:6379";
          };
          toShellVarNoOverwrite = (key: value: '': "''${${lib.toShellVar key value}}"'');

          toExportShellVar = (key: ''export ${key}'');

          mkEnvs = (
            attrset:
            lib.concatMapAttrsStringSep "\n" (key: value: ''
              ${toShellVarNoOverwrite key value}
              ${toExportShellVar key}
            '') attrset
          );

          devPackagesNoPy = with brrrScope; [
            process-compose
            redis # For the CLI
            uv
            npm-version-to-git
            nodejs
          ];
        in
        {
          default = {
            packages = devPackagesNoPy ++ [ brrrScope.python ];
            motd = ''
              This is the generic devshell for brrr development.  Use this to fix
              problems in the Python lockfile and to access generic tooling.

              Available tools:
            ''
            + lib.concatLines (map (x: "  - ${x.pname or x.name}") devPackagesNoPy)
            + ''

              For Python-specific development, use: nix develop .#python
              For TypeScript-specific development, use: nix develop .#typescript
            '';
            env = [
              {
                name = "PYTHONPATH";
                unset = true;
              }
              {
                name = "UV_PYTHON_DOWNLOADS";
                value = "never";
              }
            ];
          };
          python = {
            env = [
              {
                name = "REPO_ROOT";
                eval = "$(git rev-parse --show-toplevel)";
              }
              {
                name = "PYTHONPATH";
                unset = true;
              }
              {
                name = "UV_PYTHON_DOWNLOADS";
                value = "never";
              }
              {
                name = "UV_NO_SYNC";
                value = "1";
              }
            ];
            packages = devPackagesNoPy ++ [ brrrScope.brrrpy.brrr-venv-editable ];
            commands = [
              {
                name = "brrr-test-unit";
                category = "test";
                help = "Tests which don't need dependencies";
                command = ''
                  pytest -m 'not dependencies' "$@"
                '';
              }
              {
                name = "brrr-test-all";
                category = "test";
                help = "Tests including dependencies, make sure to run brrr-demo-deps";
                # Lol
                command = ''
                  (
                    ${mkEnvs (sharedEnvs // { AWS_DEFAULT_REGION = "us-east-1"; })}
                    exec pytest "$@"
                  )'';
              }
            ]
            ++ sharedCommands;
          };
          typescript = {
            packages = devPackagesNoPy;
            commands = [
              {
                name = "brrr-test-unit";
                category = "test";
                help = "Tests which don't need dependencies";
                command = ''
                  npm run test
                '';
              }
              {
                name = "brrr-test-all";
                category = "test";
                help = "Tests including dependencies, make sure to run brrr-demo-deps";
                command = ''
                  (
                    ${mkEnvs (sharedEnvs // { AWS_REGION = "us-east-1"; })}
                    npm run test:integration
                  )'';
              }
            ]
            ++ sharedCommands;
          };
        };
    };
}
