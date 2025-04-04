import { getOuterRadius } from "./node";
import type { APINetworkDataResponse } from "../api";

export type NodeKey = string;
export type LinkKey = string;

interface DraggableNodeBase {
    dragx: number;
    dragy: number;
    x: number;
    y: number;
    fx: number | null;
    fy: number | null;
}

export type DraggableNode = NetworkNode & DraggableNodeBase;

/**
 * Properties added to nodes by D3's force simulation
 */
interface SimulationProps extends DraggableNodeBase {
    vx: number;
    vy: number;
    index: number;
    isIntermediate: boolean;
    cluster: number;
    fixed: boolean;
    missing: number;
    hasMissing: boolean;
    links: NetworkLink[];
}

/**
 * Node type with simulation properties
 */
export type SimNode = NetworkNode & SimulationProps;

/**
 * Link type for force simulation
 */
export interface SimLink
    extends Omit<NetworkLink, "source" | "target" | "intermediate"> {
    source: SimNode;
    target: SimNode;
    role: string;
    isSpline: boolean;
    distance: number;
    intermediate: SimNode;
    // pages?: unknown;
}

/**
 * Core network data structure
 */
export interface SimData {
    center: NetworkNode;
    nodeMap: Map<NodeKey, SimNode>;
    linkMap: Map<LinkKey, SimLink>;
    maxDistance: number;
}

/**
 * Current page state and data
 */
// export interface PageData {
//     currentPage: number;
//     links: SimLink[];
//     nodes: SimNode[];
//     selectedNodeKey: NodeKey | null;
// }

export interface NetworkNode {
    key: NodeKey;
    name: string;
    type: "artist" | "label";
    size: number;
    x: number;
    y: number;
    missing: number;
    hasMissing: boolean;
    lastClickTime: number;
    lastTouchTime: number;
    distance: number;
    radius: number;
    links: NetworkLink[];
    cluster: number;
    fixed: boolean;
    isIntermediate: boolean;
    // pages?: unknown;
}

export interface NetworkLink {
    key: LinkKey;
    source: NetworkNode;
    target: NetworkNode;
    role: string;
    distance: number;
    isSpline: boolean;
    intermediate: NetworkNode;
    // pages?: unknown;
}

// Processed network data
export interface NetworkData {
    nodeMap: Map<NodeKey, NetworkNode>;
    //     nodes: NetworkNode[];
    //     links: NetworkLink[];
    center: NetworkNode;
    linkMap: Map<LinkKey, NetworkLink>;
    maxDistance: number;
    //     pageCount: number;
    //     json: {
    //         center: {
    //             key: string;
    //             name: string;
    //         };
    //         nodes: NetworkNode[];
    //         links: NetworkLink[];
    //     };
}

export interface NetworkCenter {
    center: NodeKey;
}

export const processAPINetworkDataResponse = (
    apiNetworkDataResponse: APINetworkDataResponse,
): NetworkData => {
    console.log(
        "processAPINetworkDataResponse input apiNetworkDataResponse:",
        apiNetworkDataResponse,
    );

    if (
        !apiNetworkDataResponse ||
        !Array.isArray(apiNetworkDataResponse.nodes) ||
        !Array.isArray(apiNetworkDataResponse.links)
    ) {
        throw new Error("Invalid network data format");
    }

    // Process nodes and links
    const nodeMap = new Map<NodeKey, NetworkNode>();
    const processedNodes = apiNetworkDataResponse.nodes.map((node) => {
        const size = typeof node.size === "number" ? node.size : 10;
        const distance = typeof node.distance === "number" ? node.distance : 0;
        const processedNode: NetworkNode = {
            key: node.key,
            name: node.name,
            type: node.type === "artist" ? "artist" : "label",
            size: size,
            x: 0,
            y: 0,
            distance: distance,
            radius: 0,
            links: [] as NetworkLink[],
            cluster: node.cluster,
            missing: 0,
            hasMissing: false,
            lastClickTime: 0,
            lastTouchTime: 0,
            isIntermediate: false,
            fixed: false,
            // pages: node.pages,
        };
        nodeMap.set(node.key, processedNode);
        return processedNode;
    });

    console.log("nodeMap:", nodeMap);

    const linkMap = new Map<LinkKey, NetworkLink>();
    const processedLinks = apiNetworkDataResponse.links.map((link) => {
        const source = nodeMap.get(link.source);
        const target = nodeMap.get(link.target);
        if (!source || !target) {
            console.log("Invalid link:", link);
            console.log("source:", source);
            console.log("target:", target);
            throw new Error("Invalid link: missing source or target node");
        }
        const processedLink: NetworkLink = {
            key: link.key,
            role: link.role,
            source,
            target,
            distance: Math.min(source.distance, target.distance),
            isSpline: false,
            intermediate: undefined,
            // pages: link.pages,
        };
        linkMap.set(link.key, processedLink);
        return processedLink;
    });

    console.log("processedLinks:", processedLinks);

    // Update node links after all links are processed
    processedLinks.forEach((link) => {
        const sourceNode = nodeMap.get(link.source.key);
        const targetNode = nodeMap.get(link.target.key);
        //         console.log("sourceNode:", sourceNode);
        //         console.log("targetNode:", targetNode);
        if (sourceNode && targetNode) {
            if (sourceNode.links === undefined)
                sourceNode.links = [] as NetworkLink[];
            if (targetNode.links === undefined)
                targetNode.links = [] as NetworkLink[];
            sourceNode.links.push(link);
            targetNode.links.push(link);
            //             console.log("updated sourceNode links:", sourceNode.links);
            //             console.log("updated targetNode links:", targetNode.links);
        }
    });

    console.log("updated nodeMap:", nodeMap);

    const center = processedNodes.find(
        (n) => n.key === apiNetworkDataResponse.center.key,
    );
    if (!center) {
        throw new Error("Center node not found");
    }

    const networkData: NetworkData = {
        nodeMap,
        //         nodes: processedNodes,
        //         links: processedLinks,
        center,
        linkMap,
        maxDistance: Math.max(...processedNodes.map((n) => n.distance)),
        //         pageCount: 1,
        //         json: {
        //             center: { key: center.key, name: center.name },
        //             nodes: processedNodes,
        //             links: processedLinks,
        //         },
    };
    console.log(
        "processAPINetworkDataResponse output networkData:",
        networkData,
    );

    return networkData;
};

/**
 * Processes the NetworkData data to create nodes and links for the force layout
 * @param {NetworkData} networkData - The network data object containing nodes and links
 */
export const convertNetworkDataToSimData = (
    networkData: NetworkData,
): SimData => {
    console.log("convertNetworkDataToSimData input:", networkData);

    const newNodeMap = new Map<NodeKey, SimNode>();
    const newLinkMap = new Map<LinkKey, SimLink>();
    const newSimData: SimData = {
        center: networkData.center,
        nodeMap: newNodeMap,
        linkMap: newLinkMap,
        maxDistance: 0,
    };

    // Setup node size
    networkData.nodeMap.forEach((node) => {
        const simNode: SimNode = {
            key: node.key,
            isIntermediate: false,
            size: node.size,
            name: node.name,
            type: node.type,
            x: 0,
            y: 0,
            distance: node.distance,
            radius: 0,
            missing: node.missing,
            hasMissing: node.hasMissing,
            lastClickTime: node.lastClickTime,
            lastTouchTime: node.lastTouchTime,
            links: node.links,
            cluster: node.cluster,
            fixed: node.fixed,
            vx: 0,
            vy: 0,
            index: 0,
            dragx: 0,
            dragy: 0,
            fx: null,
            fy: null,
        };
        simNode.radius = getOuterRadius(simNode);
        newNodeMap.set(node.key, simNode);
    });

    // Setup links, add intermediate node at center of link
    networkData.linkMap.forEach((link) => {
        const sourceNode = newNodeMap.get(link.source.key);
        const targetNode = newNodeMap.get(link.target.key);

        if (link.role !== "Alias") {
            const role = link.role?.toLowerCase().replace(/\s+/g, "-") ?? "";
            const intermediateNode: SimNode = {
                key: link.key,
                isIntermediate: true,
                size: 0,
                name: "",
                type: "artist", // Default type
                x: 0,
                y: 0,
                distance: 0,
                radius: 0,
                missing: 0,
                hasMissing: false,
                lastClickTime: 0,
                lastTouchTime: 0,
                links: [],
                cluster: undefined,
                fixed: false,
                vx: 0,
                vy: 0,
                index: 0,
                dragx: 0,
                dragy: 0,
                fx: null,
                fy: null,
            };

            const s2iSplineLink: SimLink = {
                isSpline: true,
                key: `${sourceNode.key}-${role}-[${targetNode.key}]`,
                source: sourceNode,
                target: intermediateNode,
                role: role,
                distance: 0,
                intermediate: undefined,
            };

            const i2tSplineLink: SimLink = {
                isSpline: true,
                key: `[${sourceNode.key}]-${role}-${targetNode.key}`,
                source: intermediateNode,
                target: targetNode,
                role: role,
                distance: 0,
                intermediate: undefined,
            };

            newNodeMap.set(intermediateNode.key, intermediateNode);
            newLinkMap.set(s2iSplineLink.key, s2iSplineLink);
            newLinkMap.set(i2tSplineLink.key, i2tSplineLink);

            const simLink: SimLink = {
                isSpline: false,
                key: link.key,
                source: sourceNode,
                target: targetNode,
                intermediate: intermediateNode,
                role: link.role,
                distance: link.distance,
            };
            newLinkMap.set(simLink.key, simLink);
        } else {
            const simLink: SimLink = {
                isSpline: false,
                key: link.key,
                source: sourceNode,
                target: targetNode,
                intermediate: undefined,
                role: link.role,
                distance: link.distance,
            };
            newLinkMap.set(simLink.key, simLink);
        }
    });

    // Update current lists of nodes and links
    //     const nodeKeysToRemove: NodeKey[] = [];
    //     Array.from(dg.network.data.nodeMap.keys()).forEach((key) => {
    //         if (!newNodeMap.has(key)) {
    //             nodeKeysToRemove.push(key);
    //         }
    //     });
    //     nodeKeysToRemove.forEach((key) => {
    //         dg.network.data.nodeMap.delete(key);
    //     });
    //
    //     const linkKeysToRemove: LinkKey[] = [];
    //     Array.from(dg.network.data.linkMap.keys()).forEach((key) => {
    //         if (!newLinkMap.has(key)) {
    //             linkKeysToRemove.push(key);
    //         }
    //     });
    //     linkKeysToRemove.forEach((key) => {
    //         dg.network.data.linkMap.delete(key);
    //     });
    //
    //     newNodeMap.forEach((newNode, newNodeKey) => {
    //         if (dg.network.data.nodeMap.has(newNodeKey)) {
    //             const oldNode = dg.network.data.nodeMap.get(newNodeKey);
    //             if (oldNode) {
    //                 oldNode.cluster = newNode.cluster;
    //                 oldNode.distance = newNode.distance;
    //                 oldNode.links = newNode.links;
    //                 oldNode.missing = newNode.missing;
    //                 oldNode.x = dg.network.newNodeCoords[0];
    //                 oldNode.y = dg.network.newNodeCoords[1];
    //             }
    //         } else {
    //             newNode.x = dg.network.newNodeCoords[0];
    //             newNode.y = dg.network.newNodeCoords[1];
    //             dg.network.data.nodeMap.set(newNodeKey, newNode);
    //         }
    //     });
    //
    //     newLinkMap.forEach((newLink, newLinkKey) => {
    //         if (!dg.network.data.linkMap.has(newLinkKey)) {
    //             //             const oldLink = dg.network.data.linkMap.get(newLinkKey);
    //             //             if (oldLink) {
    //             //                 oldLink.pages = newLink.pages;
    //             //             }
    //             //         } else {
    //             const sourceNode = dg.network.data.nodeMap.get(newLink.source.key);
    //             const targetNode = dg.network.data.nodeMap.get(newLink.target.key);
    //             if (sourceNode && targetNode) {
    //                 newLink.source = {
    //                     ...sourceNode,
    //                     links: [newLink],
    //                 };
    //                 newLink.target = {
    //                     ...targetNode,
    //                     links: [newLink],
    //                 };
    //                 if (newLink.intermediate) {
    //                     const intermediateNode = dg.network.data.nodeMap.get(
    //                         newLink.intermediate.key,
    //                     );
    //                     if (intermediateNode) {
    //                         newLink.intermediate = intermediateNode;
    //                     }
    //                 }
    //                 dg.network.data.linkMap.set(newLinkKey, newLink);
    //             }
    //         }
    //     });

    console.log("convertNetworkDataToSimData output: ", newSimData);
    return newSimData;
};
