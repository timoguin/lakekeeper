// auth.ts
import { App, ref, onMounted } from "vue";
import { UserManager, WebStorageStateStore } from "oidc-client-ts";
import * as env from "../app.config";
// Define a custom type for the user state

// OIDC Configuration
const oidcSettings = {
  authority: env.idpAuthority, // Replace with your OIDC provider authority
  client_id: env.idpClientId, // Replace with your actual client ID
  redirect_uri: `${window.location.origin}${env.idpRedirectPath}`, // Your redirect URI must match the OIDC provider settings
  response_type: "code",
  scope: env.idpScope, // Adjust as needed for your application's scopes
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
    signIn();
    const user = await userManager.getUser();
    access_token.value = user!.access_token; // Explicitly cast to custom type
    console.log("signIn", access_token.value);
    isAuthenticated.value = true;
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

// Vue Composition API hook to use authentication state and functions
export function useAuth() {
  onMounted(() => {
    initUser();
  });

  return {
    access_token: access_token.value,
    isAuthenticated: isAuthenticated.value,
    signIn,
    signOut,
  };
}

// Vue Plugin Installation Function
export default {
  install: (app: App) => {
    const auth = useAuth();
    console.log("install", isAuthenticated.value);

    app.provide("auth", auth);
    app.config.globalProperties.$auth = auth;
  },
};
