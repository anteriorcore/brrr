#!/usr/bin/env node --enable-source-maps

import { docsyncCheck } from "./cmds.ts";

await docsyncCheck();
