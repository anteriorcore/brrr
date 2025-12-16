# Copyright © 2024  Brrr Authors
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# These are all the pytest tests, with the required database dependencies spun
# up.

{ pkgs, self }:

let
  mkTest =
    { name, mkBin }:
    pkgs.testers.runNixOSTest {
      inherit name;
      globalTimeout = 5 * 60;
      nodes = {
        datastores =
          { ... }:
          {
            imports = [
              self.nixosModules.dynamodb
              ./datastores.nix
            ];
          };
        tester =
          { pkgs, ... }:
          {
            systemd.services.${name} = {
              serviceConfig = {
                Type = "oneshot";
                Restart = "no";
                RemainAfterExit = "yes";
                ExecStart = pkgs.callPackage mkBin { };
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
    };
  mkBin = {
    py =
      {
        lib,
        writeShellScriptBin,
        stdenv,
      }:
      let
        inherit (stdenv.hostPlatform) system;
      in
      lib.getExe (
        writeShellScriptBin "brrr-py-test-integration" ''
          set -euo pipefail
          ${self.packages.${system}.brrr-venv-test}/bin/pytest ${self.packages.${system}.brrr.src}
        ''
      );
    ts = { stdenv }: "${self.packages.${stdenv.hostPlatform.system}.brrrts}/bin/brrr-test-integration";
  };
in
{
  brrr-ts-test-integration = mkTest {
    name = "brrr-ts-test-integration";
    mkBin = mkBin.ts;
  };
  brrr-py-test-integration = mkTest {
    name = "brrr-py-test-integration";
    mkBin = mkBin.py;
  };
}
