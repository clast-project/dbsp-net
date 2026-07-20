# DbspNet control service — the engine container for ivm-bench (DbspNet's analogue of
# Feldera's pipeline-manager). Build from the REPO ROOT as context; engineered-wood is
# restored from nuget.org during `dotnet publish` (no git submodule needed):
#
#   docker build -f src/DbspNet.Server/Dockerfile -t dbspnet-server .
#
# At runtime it serves /deploy /resume /wait /pause /stats /healthz on :8080 and reads
# input Delta tables + writes output Delta tables at the paths given in the deploy spec
# (mounted, e.g. /data/raw/delta and /data/processed/dbspnet).

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish src/DbspNet.Server/DbspNet.Server.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "DbspNet.Server.dll"]
