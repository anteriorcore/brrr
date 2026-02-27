{ inputs, self, ... }:
{
  flake = {
    # A reusable process-compose module (for flake-parts) with either a full
    # demo environment, or just the dependencies if you want to run a server
    # manually.
    processComposeModules = {
      brrr-demo = inputs.services-flake.lib.multiService ../brrr-demo.service.nix;
      default =
        { pkgs, ... }:
        {
          imports = [ self.processComposeModules.brrr-demo ];
          services =
            let
              demoEnv = {
                AWS_DEFAULT_REGION = "us-east-1";
                AWS_REGION = "us-east-1";
                AWS_ENDPOINT_URL = "http://localhost:8000";
                AWS_ACCESS_KEY_ID = "000000000000";
                AWS_SECRET_ACCESS_KEY = "fake";
              };
              inherit (pkgs.stdenv.hostPlatform) system;
            in
            {
              redis.r1.enable = true;
              dynamodb-local.dynamodb = {
                enable = true;
                inMemory = true;
              };
              brrr-demo.server = {
                package = self.packages.${system}.brrr-demo-py;
                args = [ "web_server" ];
                environment = demoEnv;
              };
              brrr-demo.worker-py = {
                package = self.packages.${system}.brrr-demo-py;
                args = [ "brrr_worker" ];
                environment = demoEnv;
              };
              brrr-demo.worker-ts = {
                package = self.packages.${system}.brrr-demo-ts;
                environment = demoEnv;
              };
            };
        };
    };
  };

  perSystem =
    { ... }:
    {
      config = {
        process-compose.demo = {
          imports = [
            inputs.services-flake.processComposeModules.default
            self.processComposeModules.default
          ];
          cli.options.no-server = true;
          services.brrr-demo.server.enable = true;
          services.brrr-demo.worker-py.enable = true;
          services.brrr-demo.worker-ts.enable = true;
        };
        process-compose.deps = {
          imports = [
            inputs.services-flake.processComposeModules.default
            self.processComposeModules.default
          ];
          cli.options.no-server = true;
          services.brrr-demo.server.enable = false;
          services.brrr-demo.worker-py.enable = false;
          services.brrr-demo.worker-ts.enable = false;
        };
      };
    };
}
