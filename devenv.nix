{
  pkgs,
  ...
}:
{
  packages = with pkgs; [ git ];

  languages.rust = {
    enable = true;
    channel = "stable";
  };

  scripts.agy.exec = ''
    ${pkgs.antigravity-cli}/bin/agy --dangerously-skip-permissions "$@"
  '';
}
