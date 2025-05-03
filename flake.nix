{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      crane,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        lib = nixpkgs.lib;
        craneLib = crane.mkLib pkgs;

        src = lib.cleanSourceWith { src = craneLib.path ./.; };

        envVars =
          {
            RUSTY_V8_ARCHIVE =
              let
                version = "135.1.0";
                target = pkgs.hostPlatform.rust.rustcTarget;
                sha256 =
                  {
                    x86_64-linux = "sha256-QGpFNkVHO9j4uagYNC5X3JVif80RVazp63oqrdWYUoU=";
                    aarch64-linux = "sha256-J4E32qZNyqmJyFKBuU+6doRYL3ZSaEMSBlML+hSkj+o=";
                    x86_64-darwin = "sha256-UnulsDS1LlrVR2+cz+4zgWxKqbkB5ch3T9UofGCZduQ=";
                    aarch64-darwin = "sha256-mU7N/1vXzCP+mwjzLTsDkT+8YOJifwNju3Rv9Cq5Loo=";
                  }
                  .${system};
              in
              pkgs.fetchurl {
                name = "librusty_v8-${version}";
                url = "https://github.com/denoland/rusty_v8/releases/download/v${version}/librusty_v8_release_${target}.a.gz";
                inherit sha256;
              };
          }
          // (lib.attrsets.optionalAttrs pkgs.stdenv.isLinux {
            RUSTFLAGS = "-Clinker=clang -Clink-arg=--ld-path=${pkgs.mold}/bin/mold";
          });

        commonArgs = (
          {
            inherit src;
            nativeBuildInputs = with pkgs; [
              cargo
              clang
              cmake
              openssl
              perl
              pkg-config
              protobuf
              rdkafka
              rust-analyzer
              rust-bin.stable.latest.default
              rustc
            ];
            buildInputs = with pkgs; [ ] ++ lib.optionals stdenv.isDarwin [ libiconv ];
          }
          // envVars
        );
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        bin = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
          }
        );
      in
      with pkgs;
      {
        packages = {
          default = bin;
        };

        devShells.default = mkShell (
          {
            packages = [
              cargo
              cargo-watch
              cmake
              openssl
              perl
              pkg-config
              postgresql
              protobuf
              rdkafka
              rust-analyzer
              rust-bin.stable.latest.default
              rustc
              rustfmt
            ];
          }
          // envVars
        );

        formatter = nixpkgs-fmt;
      }
    );
}
