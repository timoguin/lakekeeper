<template>
  <v-btn @click="login">Login</v-btn>
</template>

<script lang="ts" setup>
import { useAuth } from "../plugins/auth";
import { idpOn } from "../app.config";
function login() {
  if (idpOn) useAuth().signIn();
}
const auth = useAuth();
onMounted(() => {
  if (idpOn) auth.initUser(); // Initialize the user on mount
});
</script>
