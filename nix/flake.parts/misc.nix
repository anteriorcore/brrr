{ inputs, ... }:
{
  flake = {
    # WIP, exporting is best effort.
    nixosModules = {
      brrr-demo = import ../brrr-demo.module.nix;
      dynamodb = import ../dynamodb.module.nix;
    };
  };

  perSystem =
    { system, ... }:
    {
      _module.args.pkgs = import inputs.nixpkgs {
        inherit system;
        config.allowUnfree = true;
      };
    };
}
