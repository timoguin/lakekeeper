const icebergCatalogUrl = import.meta.env.VITE_APP_ICEBERG_CATALOG_URL;
const idpAuthority = import.meta.env.VITE_IDP_AUTHORITY;
const idpClientId = import.meta.env.VITE_IDP_CLIENT_ID;
const idpRedirectPath = import.meta.env.VITE_IDP_REDIRECT_PATH;
const idpScope = import.meta.env.VITE_IDP_SCOPE;
const idpLogoutRedirectPath = import.meta.env
  .VITE_IDP_POST_LOGOUT_REDIRECT_PATH;
const idpOn = import.meta.env.VITE_IDP_ON.toLowerCase() === "true";

export {
  icebergCatalogUrl,
  idpAuthority,
  idpClientId,
  idpRedirectPath,
  idpScope,
  idpLogoutRedirectPath,
  idpOn,
};
