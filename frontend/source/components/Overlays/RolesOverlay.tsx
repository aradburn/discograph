/** @jsxImportSource react */
/**
 * Roles Overlay Component
 *
 * This component uses the react-arborist library for rendering
 * a tree view of roles.
 */
import React, { useEffect, useState, useRef } from "react";
import Offcanvas from "react-bootstrap/Offcanvas";
import { DOM_IDS } from "../../constants";
import type { TreeConfig } from "../../roles";
import {
    convertRolesToArboristFormat,
    updateSelectedRoleIds,
} from "../../roles";
// Import the actual component along with types
import { Tree } from "react-arborist";
import type { TreeApi } from "react-arborist";
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import type { NodeApi } from "react-arborist";
import { useResizeObserver } from "../../hooks/useResizeObserver";

interface RolesOverlayProps {
    roles?: TreeConfig;
    show: boolean;
    onHide: () => void;
}

// Data structure expected by react-arborist
interface NodeData {
    id: string | number;
    name: string;
    children?: NodeData[];
    selected?: boolean;
    isLeaf?: boolean;
}

export const RolesOverlay: React.FC<RolesOverlayProps> = ({
    roles,
    show,
    onHide,
}): React.ReactElement => {
    const treeRef = useRef<TreeApi<NodeData>>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const [arboristData, setArboristData] = useState<NodeData[]>([]);
    const [navbarHeight, setNavbarHeight] = useState<number>(0);
    const [initialOpenState, setInitialOpenState] = useState<
        Record<string, boolean>
    >({});
    // Track selected node IDs
    const [selectedIds, setSelectedIds] = useState<Set<string | number>>(
        new Set(),
    );

    // Use the resize observer to get the container height
    const { height: containerHeight = 400 } = useResizeObserver({
        ref: containerRef,
        box: "border-box",
    });

    // Effect to convert roles data to arborist format
    useEffect(() => {
        if (roles) {
            // Use the conversion function from roles.ts
            const convertedData = convertRolesToArboristFormat(
                roles,
            ) as unknown as NodeData[];

            // Create an initialOpenState map where all nodes are explicitly closed
            const openStateMap: Record<string, boolean> = {};
            // Track initially selected node IDs
            const newSelectedIds = new Set<string | number>();

            // Function to recursively process nodes and set all to closed
            const processNode = (node: NodeData): void => {
                if (node.id) {
                    openStateMap[node.id.toString()] = false;

                    // Track selected nodes
                    if (node.selected) {
                        newSelectedIds.add(node.id);
                    }
                }

                if (node.children) {
                    node.children.forEach(processNode);
                }
            };

            // Process all nodes
            convertedData.forEach((node: NodeData): void => processNode(node));

            setArboristData(convertedData);
            setInitialOpenState(openStateMap);
            setSelectedIds(newSelectedIds);

            // Set initial selected IDs in the global state
            if (newSelectedIds.size > 0) {
                updateSelectedRoleIds(Array.from(newSelectedIds));
            }
        }
    }, [roles]);

    // Effect to measure the navbar height
    useEffect(() => {
        const updateNavbarHeight = (): void => {
            const navbar = document.querySelector("nav.navbar");
            if (navbar) {
                const height = navbar.getBoundingClientRect().height;
                setNavbarHeight(height);
            }
        };

        // Initial measurement
        updateNavbarHeight();

        // Update on window resize
        window.addEventListener("resize", updateNavbarHeight);

        // Cleanup
        return (): void => {
            window.removeEventListener("resize", updateNavbarHeight);
        };
    }, []);

    // Effect to apply selection when the tree is mounted or selectedIds changes
    useEffect(() => {
        // Use a timeout to ensure the tree is fully rendered before trying to select nodes
        const timer = setTimeout(() => {
            if (treeRef.current && selectedIds.size > 0) {
                // Don't try to use the tree's selection methods at all
                // They may not be working properly
            }
        }, 300);

        return (): void => clearTimeout(timer);
    }, [selectedIds, show]);

    // If roles is undefined, we might not want to render anything
    if (!roles) {
        return <></>; // Empty fragment
    }

    const handleClose = (): void => {
        const event = new CustomEvent("discograph:hide-roles-overlay");
        window.dispatchEvent(event);
        onHide();
    };

    // Custom styles for the offcanvas component
    const offcanvasStyle = {
        top: `${navbarHeight}px`,
        height: `calc(100% - ${navbarHeight}px)`,
    };

    return (
        <Offcanvas
            id={DOM_IDS.ROLES_OVERLAY}
            show={show}
            onHide={handleClose}
            placement="start"
            style={offcanvasStyle}
            className="roles-offcanvas"
            backdropClassName="roles-backdrop"
        >
            <Offcanvas.Header closeButton>
                <Offcanvas.Title id="roles-title">Roles</Offcanvas.Title>
            </Offcanvas.Header>

            <Offcanvas.Body>
                <div
                    id={DOM_IDS.ROLES_PANEL}
                    className="roles-panel"
                    style={{ height: "100%" }}
                    ref={containerRef}
                >
                    <Tree<NodeData>
                        ref={treeRef}
                        data={arboristData}
                        rowHeight={32}
                        padding={8}
                        disableDrag={true}
                        disableDrop={true}
                        width="100%"
                        height={containerHeight - 20}
                        selection="none"
                        className="roles-tree"
                        openByDefault={false}
                        initialOpenState={initialOpenState}
                    >
                        {({ node, style, dragHandle }) => {
                            // Check if this node should be selected based on our selection state only
                            const isNodeSelected = selectedIds.has(node.id);

                            return (
                                <div
                                    style={style}
                                    ref={dragHandle}
                                    title={node.data.name}
                                    onClick={(e) => {
                                        // Prevent bubbling to avoid tree selection
                                        e.stopPropagation();
                                    }}
                                >
                                    <div
                                        style={{
                                            display: "flex",
                                            alignItems: "center",
                                            cursor: "pointer",
                                            backgroundColor: isNodeSelected
                                                ? "#e0e0e0"
                                                : "transparent",
                                            padding: "4px 8px",
                                        }}
                                    >
                                        {node.isLeaf ? (
                                            <div
                                                style={{
                                                    width: "16px",
                                                    marginRight: "8px",
                                                }}
                                            />
                                        ) : (
                                            <div
                                                style={{
                                                    width: "16px",
                                                    height: "16px",
                                                    display: "flex",
                                                    justifyContent: "center",
                                                    alignItems: "center",
                                                    marginRight: "8px",
                                                    cursor: "pointer",
                                                    borderRadius: "2px",
                                                    border: "1px solid #ccc",
                                                    fontSize: "12px",
                                                    lineHeight: 1,
                                                }}
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    node.toggle();
                                                }}
                                            >
                                                {node.isOpen ? "−" : "+"}
                                            </div>
                                        )}
                                        <input
                                            type="checkbox"
                                            style={{ marginRight: "8px" }}
                                            checked={isNodeSelected}
                                            onChange={(e) => {
                                                e.stopPropagation();
                                                // Use our controlled selection state
                                                const newSelectedIds = new Set(
                                                    selectedIds,
                                                );

                                                if (e.target.checked) {
                                                    newSelectedIds.add(node.id);
                                                } else {
                                                    newSelectedIds.delete(
                                                        node.id,
                                                    );
                                                }

                                                // Update our state
                                                setSelectedIds(newSelectedIds);
                                                // Update global state
                                                updateSelectedRoleIds(
                                                    Array.from(newSelectedIds),
                                                );
                                            }}
                                            onClick={(e) => e.stopPropagation()}
                                        />
                                        <span
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                if (!node.isLeaf) {
                                                    node.toggle();
                                                }
                                            }}
                                        >
                                            {node.data.name}
                                        </span>
                                    </div>
                                </div>
                            );
                        }}
                    </Tree>

                    {/* Temporary placeholder that displays when no data is available */}
                    {arboristData.length === 0 && show && (
                        <div
                            id={DOM_IDS.ROLES_CONTAINER}
                            className="roles-container"
                        >
                            <p>Loading roles data...</p>
                        </div>
                    )}
                </div>
            </Offcanvas.Body>
        </Offcanvas>
    );
};
