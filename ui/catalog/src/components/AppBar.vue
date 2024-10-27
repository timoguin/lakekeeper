<template>
  <v-app-bar :elevation="2">
    <template v-slot:prepend>
      <v-app-bar-nav-icon :icon="navIcon" @click="navBar"></v-app-bar-nav-icon>
    </template>

    <v-app-bar-title>Lakekeeper</v-app-bar-title>
    <v-list-item @click="dialog = true">
      <v-list-item-title>
        <div class="text-center pa-4">
          <v-dialog
            v-model="dialog"
            transition="dialog-bottom-transition"
            fullscreen
          >
            <template v-slot:activator="{ props: activatorProps }">
              <v-btn
                prepend-icon="mdi-home-silo"
                size="small"
                text="Default Project"
                v-bind="activatorProps"
              ></v-btn>
            </template>

            <v-card>
              <v-toolbar>
                <v-btn icon="mdi-close" @click="dialog = false"></v-btn>

                <v-toolbar-title>Default Project</v-toolbar-title>

                <v-spacer></v-spacer>

                <v-toolbar-items>
                  <v-btn
                    text="Save"
                    variant="text"
                    @click="dialog = false"
                  ></v-btn>
                </v-toolbar-items>
              </v-toolbar>

              <v-list lines="two" subheader>
                <v-list-subheader>User Controls</v-list-subheader>

                <v-list-item
                  subtitle="Set the content filtering level to restrict apps that can be downloaded"
                  title="Content filtering"
                  link
                ></v-list-item>

                <v-list-item
                  subtitle="Require password for purchase or use password to restrict purchase"
                  title="Password"
                  link
                ></v-list-item>

                <v-divider></v-divider>

                <v-list-subheader>General</v-list-subheader>

                <v-list-item
                  subtitle="Notify me about updates to apps or games that I downloaded"
                  title="Notifications"
                  @click="notifications = !notifications"
                >
                  <template v-slot:prepend>
                    <v-list-item-action start>
                      <v-checkbox-btn
                        v-model="notifications"
                        color="primary"
                      ></v-checkbox-btn>
                    </v-list-item-action>
                  </template>
                </v-list-item>

                <v-list-item
                  subtitle="Auto-update apps at any time. Data charges may apply"
                  title="Sound"
                  @click="sound = !sound"
                >
                  <template v-slot:prepend>
                    <v-list-item-action start>
                      <v-checkbox-btn
                        v-model="sound"
                        color="primary"
                      ></v-checkbox-btn>
                    </v-list-item-action>
                  </template>
                </v-list-item>

                <v-list-item
                  subtitle="Automatically add home screen widgets"
                  title="Auto-add widgets"
                  @click="widgets = !widgets"
                >
                  <template v-slot:prepend>
                    <v-list-item-action start>
                      <v-checkbox-btn
                        v-model="widgets"
                        color="primary"
                      ></v-checkbox-btn>
                    </v-list-item-action>
                  </template>
                </v-list-item>
              </v-list>
            </v-card>
          </v-dialog></div
      ></v-list-item-title>
    </v-list-item>
    <v-spacer></v-spacer>

    <v-menu open-on-hover v-if="userStorage.isAuthenticated">
      <template #activator="{ props }">
        <v-btn v-bind="props"> <v-icon>mdi-account</v-icon> </v-btn>
      </template>
      <v-list>
        <v-list-item>
          <v-list-item-title>
            {{ userStorage.user.given_name }}
            {{ userStorage.user.family_name }}
            <v-btn
              @click="toggleTheme"
              size="x-small"
              :icon="themeLight ? 'mdi-lightbulb-off' : 'mdi-lightbulb-on'"
              variant="text"
            ></v-btn>
          </v-list-item-title>
        </v-list-item>

        <v-divider></v-divider>

        <v-list-item @click="goToUserProfile">
          <v-list-item-title>User Profile</v-list-item-title>
        </v-list-item>

        <v-divider class="mt-2"></v-divider>

        <v-list-item @click="logout">
          <template #prepend>
            <v-icon icon="mdi-logout"></v-icon>
          </template>
          <v-list-item-title>Logout</v-list-item-title>
        </v-list-item>
      </v-list>
    </v-menu>

    <v-menu open-on-hover v-if="!idpOn">
      <template #activator="{ props }">
        <v-btn v-bind="props"> <v-icon>mdi-account</v-icon> </v-btn>
      </template>
      <v-list>
        <v-list-item>
          <v-list-item-title>
            {{ userStorage.user.given_name }}
            {{ userStorage.user.family_name }}
            <v-btn
              @click="toggleTheme"
              size="x-small"
              :icon="themeLight ? 'mdi-lightbulb-off' : 'mdi-lightbulb-on'"
              variant="text"
            ></v-btn>
          </v-list-item-title>
        </v-list-item>
      </v-list>
    </v-menu>
  </v-app-bar>
</template>

<script setup lang="ts">
import { shallowRef } from "vue";
import { useTheme } from "vuetify";
import { useAuth } from "../plugins/auth";
import { useVisualStore } from "../stores/visual";
import { idpOn } from "../app.config";

const router = useRouter();
const visual = useVisualStore();

import { useUserStore } from "../stores/user";
const userStorage = useUserStore();

const dialog = shallowRef(false);
const notifications = shallowRef(false);
const sound = shallowRef(true);
const widgets = shallowRef(false);

const theme = useTheme();
const themeLight = computed(() => {
  return visual.themeLight;
});

const themeText = computed(() => {
  return themeLight.value ? "light" : "dark";
});

const navIcon = computed(() => {
  return visual.navBarShow ? "mdi-menu-open" : "mdi-menu";
});

ref("mdi-menu");

onMounted(() => {
  theme.global.name.value = themeText.value;
});
function toggleTheme() {
  visual.toggleThemeLight();
  theme.global.name.value = themeText.value;
}

function navBar() {
  visual.navBarSwitch();
}

function logout() {
  userStorage.unsetUser();
  useAuth().signOut();
}

function goToUserProfile() {
  router.push("/user-profile");
}
</script>
