{ self, inputs, ... }:
{
  perSystem =
    { pkgs, lib, ... }:
    {
      checks =
        lib.mapAttrs
          (
            name: mkBin:
            pkgs.testers.runNixOSTest {
              inherit name;
              globalTimeout = 5 * 60;
              nodes = {
                datastores =
                  { ... }:
                  {
                    imports = [
                      self.nixosModules.dynamodb
                      ../datastores.nix
                    ];
                  };
                tester =
                  { pkgs, config, ... }:
                  {
                    imports = [
                      ../nixos-brrr-scope.nix
                      { _module.args = { inherit inputs; }; }
                    ];
                    systemd.services.${name} = {
                      serviceConfig = {
                        Type = "oneshot";
                        Restart = "no";
                        RemainAfterExit = "yes";
                        ExecStart = pkgs.callPackage mkBin { inherit config; };
                      };
                      environment = {
                        AWS_DEFAULT_REGION = "us-east-1";
                        AWS_REGION = "us-east-1";
                        AWS_ENDPOINT_URL = "http://datastores:8000";
                        AWS_ACCESS_KEY_ID = "fake";
                        AWS_SECRET_ACCESS_KEY = "fake";
                        BRRR_TEST_REDIS_URL = "redis://datastores:6379";
                      };
                      enable = true;
                      wants = [ "multi-user.target" ];
                    };
                  };
              };
              testScript = ''
                datastores.wait_for_unit("default.target")
                tester.wait_for_unit("default.target")
                tester.systemctl("start --no-block ${name}.service")
                tester.wait_for_unit("${name}.service")
              '';
            }
          )
          {
            brrr-ts-test-integration =
              { config, ... }:
              let
                inherit (config.brrr.scope) brrrts;
              in
              "${brrrts}/bin/brrr-test-integration";
            brrr-py-test-integration =
              {
                config,
                lib,
                writeShellScriptBin,
                ...
              }:
              let
                inherit (config.brrr.scope) brrr brrr-venv-test;
              in
              lib.getExe (
                writeShellScriptBin "brrr-py-test-integration" ''
                  set -euo pipefail
                  ${brrr-venv-test}/bin/pytest ${brrr.src}
                ''
              );
          };
    };
}
