<template>
  <v-app-bar :elevation="2">
    <template v-slot:prepend>
      <v-app-bar-nav-icon :icon="navIcon" @click="navBar"></v-app-bar-nav-icon>
    </template>

    <v-app-bar-title>Lakekeeper</v-app-bar-title>
    <v-list-item
      v-if="userStorage.isAuthenticated && visual.project.bootstrapped"
      @click="dialog = true"
    >
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
                :text="project['project-name']"
                v-bind="activatorProps"
              ></v-btn>
            </template>

            <v-card>
              <v-toolbar>
                <v-btn icon="mdi-close" @click="dialog = false"></v-btn>

                <v-toolbar-title>{{ project["project-name"] }}</v-toolbar-title>

                <v-spacer></v-spacer>
              </v-toolbar>

              <v-list lines="two" subheader>
                <v-list-subheader>Selected Project </v-list-subheader>

                <v-list-item
                  :subtitle="`ID: ${project['project-id']}`"
                  :title="`Name: ${project['project-name']}`"
                  link
                ></v-list-item>

                <v-divider class="mt-8"></v-divider>

                <v-list-subheader>Available Projects</v-list-subheader>

                <v-list-item
                  v-for="p in projectList"
                  :key="p['project-id']"
                  :subtitle="`ID: ${project['project-id']}`"
                  :title="`Name: ${project['project-name']}`"
                  link
                >
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
import { useUserStore } from "../stores/user";
import { useFunctions } from "../plugins/functions";

const router = useRouter();
const visual = useVisualStore();
const functions = useFunctions();
const userStorage = useUserStore();
const project = computed(() => {
  return visual.projectSelected;
});

const projectList = computed(() => {
  return visual.projectList;
});
const dialog = shallowRef(false);
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

onMounted(async () => {
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

watch(dialog, async (n, o) => {
  if (n) functions.loadProjectList();
});
</script>
