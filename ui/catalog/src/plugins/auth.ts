import { App, ref } from "vue";
import { UserManager, WebStorageStateStore } from "oidc-client-ts";

import * as env from "../app.config";

// OIDC Configuration
const oidcSettings = {
  authority: env.idpAuthority,
  client_id: env.idpClientId,
  redirect_uri: `${window.location.origin}${env.idpRedirectPath}`,
  response_type: "code",
  scope: env.idpScope,
  post_logout_redirect_uri: `${window.location.origin}${env.idpLogoutRedirectPath}`,
  userStore: new WebStorageStateStore({ store: window.localStorage }),
};

// Initialize UserManager
const userManager = new UserManager(oidcSettings);

// Define reactive state
const access_token = ref("");
const isAuthenticated = ref(false);

// Helper functions
const initUser = async () => {
  try {
    await signIn(); // Ensure signIn is called as part of initialization
    const user = await userManager.getUser();
    if (user) {
      access_token.value = user.access_token; // Use non-null assertion if user is expected to exist
      console.log("signIn", access_token.value);
      isAuthenticated.value = true;
    }
  } catch (error) {
    console.error("Failed to initialize OIDC user", error);
  }
};

const signIn = async () => {
  try {
    await userManager.signinRedirect();
  } catch (error) {
    console.error("OIDC sign-in failed", error);
  }
};

const signOut = async () => {
  try {
    await userManager.signoutRedirect();

    access_token.value = "";
    isAuthenticated.value = false;
  } catch (error) {
    console.error("OIDC sign-out failed", error);
  }
};

const refreshToken = async () => {
  try {
    await userManager.signinSilent();
  } catch (error) {
    console.error(error);
  }
};

// Vue Composition API hook to use authentication state and functions
export function useAuth() {
  return {
    access_token,
    isAuthenticated,
    refreshToken,
    signIn,
    signOut,
    initUser, // Expose initUser for calling in components
  };
}

// Vue Plugin Installation Function
export default {
  install: (app: App) => {
    const auth = useAuth();
    app.provide("auth", auth);
    app.config.globalProperties.$auth = auth;
  },
};
