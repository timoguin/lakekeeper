<template>
  <div>Login</div>
  <v-btn @click="login">Login</v-btn>
</template>

<script lang="ts" setup>
import { useAuth } from "../plugins/auth";
import { idpOn } from "../app.config";
function login() {
  if (idpOn) useAuth().signIn();
}
</script>
