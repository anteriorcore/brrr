{ self, inputs, ... }:
{
  perSystem =
    { pkgs, ... }:
    {
      checks =
        let
          mkTest =
            { nodes, name }:
            pkgs.testers.runNixOSTest {
              inherit name;
              nodes = nodes // {
                datastores =
                  { ... }:
                  {
                    imports = [
                      self.nixosModules.dynamodb
                      ../datastores.nix
                    ];
                  };
                # Separate node entirely just for the actual testing
                tester =
                  { pkgs, ... }:
                  let
                    test-script = pkgs.writeShellApplication {
                      name = "test-brrr-demo";
                      text = ''
                        >/dev/stderr echo "Fetching results from previously scheduled tasks..."
                        json="$(curl --fail -sSL "http://server:8080/hello?greetee=Jim")"
                        val="$(<<<"$json" jq '. == {status: "ok", result: "Hello, Jim!"}')"
                        [[ "$val" == true ]]
                        json="$(curl --fail -sSL "http://server:8080/calc_and_print?op=fib&n=78&salt=abcd")"
                        val="$(<<<"$json" jq '. == {status: "ok", result: 8944394323791464}')"
                        [[ "$val" == true ]]
                        json="$(curl --fail -sSL "http://server:8080/calc_and_print?op=lucas&n=76&salt=abcd")"
                        val="$(<<<"$json" jq '. == {status: "ok", result: 7639424778862807}')"
                        [[ "$val" == true ]]
                      '';
                    };
                  in
                  {
                    imports = [
                      ../nixos-brrr-scope.nix
                      { _module.args = { inherit inputs; }; }
                    ];
                    environment.systemPackages = [
                      test-script
                    ]
                    ++ (with pkgs; [
                      curl
                      jq
                    ]);
                  };
              };

              globalTimeout = 10 * 60;

              # Chose a big number to ensure debouncing works.
              # fib(78) = 8944394323791464 and lucas(76) = 7639424778862807 are the largest numbers under JavaScript's Number.MAX_SAFE_INTEGER
              testScript = ''
                # Start first because it's a dependency
                datastores.wait_for_unit("default.target")
                # Server initializes the stores
                server.wait_for_unit("default.target")
                pyworker.wait_for_unit("default.target")
                tsworker.wait_for_unit("default.target")
                tester.wait_for_unit("default.target")
                server.wait_for_open_port(8080)
                tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/hello?greetee=Jim'")
                tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/calc_and_print?op=fib&n=78&salt=abcd'")
                tester.wait_until_succeeds("curl --fail -sSL -X POST 'http://server:8080/calc_and_print?op=lucas&n=76&salt=abcd'")
                tester.wait_until_succeeds("test-brrr-demo")
              '';
            };
          demoEnvs = {
            BRRR_DEMO_LISTEN_HOST = "0.0.0.0";
            BRRR_DEMO_REDIS_URL = "redis://datastores:6379";
            AWS_REGION = "foo";
            AWS_DEFAULT_REGION = "foo";
            AWS_ENDPOINT_URL = "http://datastores:8000";
            AWS_ACCESS_KEY_ID = "foo";
            AWS_SECRET_ACCESS_KEY = "bar";
          };
          module = {
            server =
              { config, ... }:
              {
                imports = [
                  self.nixosModules.brrr-demo
                  ../nixos-brrr-scope.nix
                  { _module.args = { inherit inputs; }; }
                ];
                networking.firewall.allowedTCPPorts = [ 8080 ];
                services.brrr-demo = {
                  enable = true;
                  package = config.brrr.scope.brrr-demo-py;
                  args = [ "web_server" ];
                  environment = demoEnvs;
                };
              };
            pyworker =
              { config, ... }:
              {
                imports = [
                  self.nixosModules.brrr-demo
                  ../nixos-brrr-scope.nix
                  { _module.args = { inherit inputs; }; }
                ];
                services.brrr-demo = {
                  enable = true;
                  package = config.brrr.scope.brrr-demo-py;
                  args = [ "brrr_worker" ];
                  environment = demoEnvs;
                };
              };
            tsworker =
              { config, ... }:
              {
                imports = [
                  self.nixosModules.brrr-demo
                  ../nixos-brrr-scope.nix
                  { _module.args = { inherit inputs; }; }
                ];
                services.brrr-demo = {
                  enable = true;
                  package = config.brrr.scope.brrr-demo-ts;
                  environment = demoEnvs;
                };
              };
          };
          docker = {
            server =
              { config, ... }:
              {
                imports = [
                  ../nixos-brrr-scope.nix
                  { _module.args = { inherit inputs; }; }
                ];
                # Podman (default backend) doesn’t like images built with nix
                # apparently.  Ironic!
                virtualisation.oci-containers.backend = "docker";
                virtualisation.oci-containers.containers.brrr = {
                  extraOptions = [ "--network=host" ];
                  image = "brrr-demo-py:latest";
                  imageFile = config.brrr.scope.docker-py;
                  environment = demoEnvs;
                  cmd = [ "web_server" ];
                };
                networking.firewall.allowedTCPPorts = [ 8080 ];
              };
            pyworker =
              { config, ... }:
              {
                imports = [
                  ../nixos-brrr-scope.nix
                  { _module.args = { inherit inputs; }; }
                ];
                virtualisation.oci-containers.backend = "docker";
                virtualisation.oci-containers.containers.brrr = {
                  extraOptions = [ "--network=host" ];
                  image = "brrr-demo-py:latest";
                  imageFile = config.brrr.scope.docker-py;
                  cmd = [ "brrr_worker" ];
                  environment = demoEnvs;
                };
              };
            tsworker =
              { config, ... }:
              {
                imports = [
                  ../nixos-brrr-scope.nix
                  { _module.args = { inherit inputs; }; }
                ];
                virtualisation.oci-containers.backend = "docker";
                virtualisation.oci-containers.containers.brrr-ts = {
                  extraOptions = [ "--network=host" ];
                  image = "brrr-demo-ts:latest";
                  imageFile = config.brrr.scope.docker-ts;
                  environment = demoEnvs;
                };
              };
          };
        in
        {
          docker-test = mkTest {
            name = "brrr-demo-docker";
            nodes = docker;
          };
          nixos-module-test = mkTest {
            name = "brrr-demo-nixos";
            nodes = module;
          };
        };
    };
}
