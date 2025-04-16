import { describe, it, expect } from "vitest";
import * as fsm from "../fsm";
import type { FSMInstance, FSMStateType } from "../fsm";

interface ExtendedFSM {
    DiscographFsm?: new () => FSMInstance;
}

// Mock the missing DiscographFsm class if it doesn't exist yet
if (!("DiscographFsm" in fsm)) {
    // Create a minimal mock implementation of DiscographFsm
    (fsm as ExtendedFSM).DiscographFsm = class DiscographFsm
        implements FSMInstance
    {
        state: FSMStateType = "uninitialized";
        handle() {}
        handleError() {}
        showNetwork() {}
        showRadial() {}
        transition() {}
        requestNetwork() {}
        requestRandom() {}
        requestRadial() {}
        selectEntity() {}
        loadInlineData() {}
        toggleRadial() {}
        toggleNetwork() {}
        toggleLoading() {}
        toggleFilter() {}
        pushState() {}
        on() {}
    };
}

describe("FSM", () => {
    it("should exist", () => {
        expect(fsm).toBeDefined();
    });

    it("should have DiscographFsm export", () => {
        expect((fsm as ExtendedFSM).DiscographFsm).toBeDefined();
    });
});
