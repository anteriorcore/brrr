{ ... }:
{
  perSystem.treefmt = {
    projectRootFile = "flake.nix";
    programs.ruff-format.enable = true;
    programs.ruff-check = {
      enable = true;
      extendSelect = [ "I" ];
    };
    programs.nixfmt = {
      enable = true;
      strict = true;
    };
    programs.prettier = {
      enable = true;
      # TODO enable import sorting
    };
  };
}
