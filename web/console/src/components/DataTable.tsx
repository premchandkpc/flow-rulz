import { useMemo, useState, useCallback, useRef, ReactNode, KeyboardEvent } from "react";

export interface Column<T> {
  key: string;
  header: string;
  render?: (row: T, rowIndex: number) => ReactNode;
  width?: string;
  minWidth?: string;
  maxWidth?: string;
  align?: "left" | "center" | "right";
  sortable?: boolean;
  filterable?: boolean;
  cellClassName?: string;
  headerClassName?: string;
}

export interface DataTableProps<T> {
  data: T[];
  columns: Column<T>[];
  keyExtractor: (row: T) => string;
  onRowClick?: (row: T) => void;
  onRowDoubleClick?: (row: T) => void;
  selection?: Set<string>;
  onSelectionChange?: (selection: Set<string>) => void;
  emptyMessage?: string;
  loading?: boolean;
  stickyHeader?: boolean;
  maxHeight?: string;
  sortColumn?: string;
  sortDirection?: "asc" | "desc";
  onSort?: (column: string, direction: "asc" | "desc") => void;
  filterValues?: Record<string, string>;
  onFilterChange?: (column: string, value: string) => void;
  className?: string;
  rowClassName?: (row: T) => string;
  footer?: ReactNode;
  pagination?: {
    page: number;
    pageSize: number;
    total: number;
    onPageChange: (page: number) => void;
    onPageSizeChange: (pageSize: number) => void;
  };
  toolbar?: ReactNode;
  rowActions?: Array<{
    label: string;
    icon?: string;
    onClick: (row: T, event: React.MouseEvent) => void;
    className?: string;
    disabled?: (row: T) => boolean;
    hidden?: (row: T) => boolean;
  }>;
}

function SortIcon({ direction }: { direction: "asc" | "desc" }) {
  return (
    <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
      {direction === "asc" ? (
        <path d="M6 2L10 6H2L6 2Z" fill="currentColor" />
      ) : (
        <path d="M6 10L2 6H10L6 10Z" fill="currentColor" />
      )}
    </svg>
  );
}

function FilterIcon({ active }: { active: boolean }) {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none" style={{ opacity: active ? 1 : 0.4 }}>
      <path d="M7 1.5L12.5 7.5H1.5L7 1.5Z" stroke="currentColor" strokeWidth="1.5" />
      <path d="M3 10.5H11" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
    </svg>
  );
}

function Checkbox({ checked, indeterminate, onChange, disabled }: { 
  checked: boolean; 
  indeterminate?: boolean; 
  onChange?: (checked: boolean) => void;
  disabled?: boolean;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  if (indeterminate && inputRef.current) {
    inputRef.current.indeterminate = true;
  }
  return (
    <input
      ref={inputRef}
      type="checkbox"
      checked={checked}
      onChange={(e) => onChange?.(e.target.checked)}
      disabled={disabled}
      className="datatable-checkbox"
      style={{
        width: "16px",
        height: "16px",
        accentColor: "#4263eb",
        cursor: disabled ? "not-allowed" : "pointer",
      }}
    />
  );
}

export function DataTable<T>({
  data,
  columns,
  keyExtractor,
  onRowClick,
  onRowDoubleClick,
  selection,
  onSelectionChange,
  emptyMessage = "No data",
  loading = false,
  stickyHeader = true,
  maxHeight = "600px",
  sortColumn,
  sortDirection,
  onSort,
  filterValues = {},
  onFilterChange,
  className = "",
  rowClassName,
  footer,
  pagination,
  toolbar,
  rowActions,
}: DataTableProps<T>) {
  const [hoveredRow, setHoveredRow] = useState<string | null>(null);
  const [filterFocus, setFilterFocus] = useState<string | null>(null);

  const selectedKeys = selection || new Set();
  const allSelected = data.length > 0 && data.every(row => selectedKeys.has(keyExtractor(row)));
  const someSelected = data.some(row => selectedKeys.has(keyExtractor(row)));

  const handleSelectAll = useCallback((checked: boolean) => {
    if (onSelectionChange) {
      const newSelection = new Set(selectedKeys);
      if (checked) {
        data.forEach(row => newSelection.add(keyExtractor(row)));
      } else {
        data.forEach(row => newSelection.delete(keyExtractor(row)));
      }
      onSelectionChange(newSelection);
    }
  }, [data, keyExtractor, onSelectionChange, selectedKeys]);

  const handleRowSelect = useCallback((rowKey: string, checked: boolean) => {
    if (onSelectionChange) {
      const newSelection = new Set(selectedKeys);
      if (checked) newSelection.add(rowKey);
      else newSelection.delete(rowKey);
      onSelectionChange(newSelection);
    }
  }, [onSelectionChange, selectedKeys]);

  const handleSort = useCallback((columnKey: string) => {
    if (!onSort) return;
    const column = columns.find(c => c.key === columnKey);
    if (!column?.sortable) return;
    const direction = sortColumn === columnKey && sortDirection === "asc" ? "desc" : "asc";
    onSort(columnKey, direction);
  }, [columns, onSort, sortColumn, sortDirection]);

  const handleFilter = useCallback((columnKey: string, value: string) => {
    onFilterChange?.(columnKey, value);
  }, [onFilterChange]);

  const handleKeyDown = useCallback((event: KeyboardEvent<HTMLTableRowElement>, row: T, rowKey: string) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onRowClick?.(row);
    }
    if (event.key === " " && event.shiftKey) {
      event.preventDefault();
      handleRowSelect(rowKey, !selectedKeys.has(rowKey));
    }
  }, [onRowClick, handleRowSelect, selectedKeys]);

  if (loading) {
    return (
      <div className={`datatable-container ${className}`} style={{ maxHeight }}>
        <div className="datatable-loading" role="status" aria-label="Loading data">
          <div className="datatable-spinner" />
          <span>Loading…</span>
        </div>
      </div>
    );
  }

  return (
    <div className={`datatable-container ${className}`} style={{ maxHeight, display: "flex", flexDirection: "column" }}>
      {toolbar && <div className="datatable-toolbar">{toolbar}</div>}
      
      <div className="datatable-wrapper" style={{ overflow: "auto", position: "relative" }}>
        <table className="datatable" role="grid">
          <thead className={stickyHeader ? "datatable-sticky-header" : ""}>
            <tr role="row">
              {(selection || rowActions) && (
                <th className="datatable-select-col" style={{ width: "48px" }}>
                  <Checkbox
                    checked={allSelected}
                    indeterminate={someSelected && !allSelected}
                    onChange={handleSelectAll}
                  />
                </th>
              )}
              {columns.map((column, colIndex) => (
                <th
                  key={column.key}
                  className={`datatable-th ${column.headerClassName || ""} ${column.sortable ? "sortable" : ""} ${column.filterable ? "filterable" : ""}`}
                  style={{
                    width: column.width,
                    minWidth: column.minWidth,
                    maxWidth: column.maxWidth,
                    textAlign: column.align || "left",
                    position: stickyHeader ? "sticky" : undefined,
                    top: 0,
                    zIndex: 10,
                    background: "var(--surface, #fff)",
                  }}
                  role="columnheader"
                  aria-sort={sortColumn === column.key ? (sortDirection === "asc" ? "ascending" : "descending") : "none"}
                >
                  <div className="datatable-th-content" style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                    <span>{column.header}</span>
                    {column.sortable && (
                      <button
                        className="datatable-sort-btn"
                        onClick={() => handleSort(column.key)}
                        aria-label={`Sort by ${column.header} ${sortColumn === column.key ? (sortDirection === "asc" ? "descending" : "ascending") : "ascending"}`}
                        style={{ padding: "2px", marginLeft: "auto", background: "transparent", border: "none", cursor: "pointer", color: "var(--muted, #7a8699)" }}
                      >
                        <SortIcon direction={sortColumn === column.key && sortDirection ? sortDirection : "asc"} />
                      </button>
                    )}
                  </div>
                  {column.filterable && (
                    <input
                      type="text"
                      className="datatable-filter-input"
                      placeholder={`Filter ${column.header}…`}
                      value={filterValues[column.key] || ""}
                      onChange={(e) => handleFilter(column.key, e.target.value)}
                      onFocus={() => setFilterFocus(column.key)}
                      onBlur={() => setFilterFocus(null)}
                      style={{
                        width: "100%",
                        marginTop: "6px",
                        padding: "4px 8px",
                        fontSize: "11px",
                        border: "1px solid var(--line, #d9dee7)",
                        borderRadius: "4px",
                        background: filterFocus === column.key ? "white" : "#fafbfc",
                        outline: "none",
                        boxShadow: filterFocus === column.key ? "0 0 0 2px #4263eb33" : "none",
                      }}
                    />
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="datatable-tbody">
            {data.length === 0 ? (
              <tr>
                <td colSpan={columns.length + ((selection || rowActions) ? 1 : 0)} className="datatable-empty" style={{ textAlign: "center", padding: "48px 24px", color: "var(--muted, #7a8699)" }}>
                  {emptyMessage}
                </td>
              </tr>
            ) : (
              data.map((row, rowIndex) => {
                const rowKey = keyExtractor(row);
                const isSelected = selectedKeys.has(rowKey);
                return (
                  <tr
                    key={rowKey}
                    className={`datatable-tr ${isSelected ? "selected" : ""} ${hoveredRow === rowKey ? "hovered" : ""} ${rowClassName?.(row) || ""}`}
                    style={{
                      cursor: onRowClick || onRowDoubleClick || rowActions ? "pointer" : "default",
                    }}
                    onClick={() => onRowClick?.(row)}
                    onDoubleClick={() => onRowDoubleClick?.(row)}
                    onMouseEnter={() => setHoveredRow(rowKey)}
                    onMouseLeave={() => setHoveredRow(null)}
                    onKeyDown={(e) => handleKeyDown(e, row, rowKey)}
                    tabIndex={onRowClick || onRowDoubleClick ? 0 : undefined}
                    role="row"
                    aria-selected={isSelected}
                  >
                    {(selection || rowActions) && (
                      <td className="datatable-select-col">
                        {selection && (
                          <Checkbox
                            checked={isSelected}
                            onChange={(checked) => handleRowSelect(rowKey, checked)}
                          />
                        )}
                      </td>
                    )}
                    {columns.map((column) => (
                      <td
                        key={column.key}
                        className={`datatable-td ${column.cellClassName || ""}`}
                        style={{
                          textAlign: column.align || "left",
                          maxWidth: column.maxWidth,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                        role="gridcell"
                      >
                        {column.render 
                          ? column.render(row, rowIndex) 
                          : String((row as any)[column.key] ?? "")}
                      </td>
                    ))}
                    {rowActions && rowActions.length > 0 && (
                      <td className="datatable-actions-col" style={{ width: "1px", whiteSpace: "nowrap" }}>
                        <div className="datatable-row-actions" style={{ display: "flex", gap: "4px", justifyContent: "flex-end" }}>
                          {rowActions.map((action, idx) => (
                            !action.hidden?.(row) && (
                              <button
                                key={idx}
                                className={`button ${action.className || "text-button"} ${action.disabled?.(row) ? "disabled" : ""}`}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  if (!action.disabled?.(row)) action.onClick(row, e);
                                }}
                                disabled={action.disabled?.(row)}
                                style={{ padding: "4px 8px", fontSize: "11px" }}
                              >
                                {action.icon && <span style={{ marginRight: "4px" }}>{action.icon}</span>}
                                {action.label}
                              </button>
                            )
                          ))}
                        </div>
                      </td>
                    )}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {footer && <div className="datatable-footer" style={{ padding: "12px 16px", borderTop: "1px solid var(--line, #d9dee7)", background: "#fafbfc" }}>{footer}</div>}

      {pagination && (
        <div className="datatable-pagination" style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "12px 16px", borderTop: "1px solid var(--line, #d9dee7)", background: "#fafbfc", flexWrap: "wrap", gap: "12px" }}>
          <div className="datatable-pagination-info" style={{ fontSize: "12px", color: "var(--muted, #7a8699)" }}>
            Showing {Math.min((pagination.page - 1) * pagination.pageSize + 1, pagination.total)}–{Math.min(pagination.page * pagination.pageSize, pagination.total)} of {pagination.total}
          </div>
          <div className="datatable-pagination-controls" style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <select
              value={pagination.pageSize}
              onChange={(e) => pagination.onPageSizeChange(Number(e.target.value))}
              style={{ padding: "4px 8px", fontSize: "11px", border: "1px solid var(--line, #d9dee7)", borderRadius: "4px", background: "white" }}
            >
              {[10, 25, 50, 100].map(size => <option key={size} value={size}>{size} per page</option>)}
            </select>
            <button
              className="button text-button"
              onClick={() => pagination.onPageChange(pagination.page - 1)}
              disabled={pagination.page === 1}
              style={{ padding: "4px 10px" }}
            >
              Previous
            </button>
            <span style={{ fontSize: "12px", color: "var(--muted, #7a8699)", minWidth: "60px", textAlign: "center" }}>
              Page {pagination.page} of {Math.ceil(pagination.total / pagination.pageSize)}
            </span>
            <button
              className="button text-button"
              onClick={() => pagination.onPageChange(pagination.page + 1)}
              disabled={pagination.page >= Math.ceil(pagination.total / pagination.pageSize)}
              style={{ padding: "4px 10px" }}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export function createColumn<T>(key: string, header: string, options: Partial<Column<T>> = {}): Column<T> {
  return { key, header, ...options };
}

export function renderBadge(text: string, variant: "default" | "success" | "warning" | "error" | "info" = "default") {
  const styles: Record<string, string> = {
    default: "bg-gray-100 text-gray-700",
    success: "bg-green-100 text-green-700",
    warning: "bg-amber-100 text-amber-700",
    error: "bg-red-100 text-red-700",
    info: "bg-blue-100 text-blue-700",
  };
  return <span className={`datatable-badge ${styles[variant]}`}>{text}</span>;
}

export function renderStatus(status: string) {
  const normalized = status.toLowerCase();
  if (["completed", "delivered", "active", "online", "healthy", "success"].includes(normalized)) {
    return renderBadge(status, "success");
  }
  if (["pending", "queued", "processing", "running", "waiting"].includes(normalized)) {
    return renderBadge(status, "info");
  }
  if (["failed", "error", "offline", "unhealthy", "dead"].includes(normalized)) {
    return renderBadge(status, "error");
  }
  if (["quarantined", "blocked", "rejected", "denied"].includes(normalized)) {
    return renderBadge(status, "warning");
  }
  return renderBadge(status, "default");
}