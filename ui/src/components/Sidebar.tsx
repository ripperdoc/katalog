import type { ReactNode } from "react";

type SidebarProps = {
  isOpen: boolean;
  title: string;
  subtitle?: string;
  onClose: () => void;
  children: ReactNode;
};

const Sidebar = ({ isOpen, title, subtitle, onClose, children }: SidebarProps) => {
  if (!isOpen) {
    return null;
  }

  return (
    <div className="sidebar-overlay" onClick={onClose}>
      <aside className="sidebar-panel" onClick={(event) => event.stopPropagation()}>
        <header className="sidebar-header">
          <div>
            <h3>{title}</h3>
            {subtitle && <p>{subtitle}</p>}
          </div>
          <button className="app-btn btn-primary" type="button" onClick={onClose}>
            Close
          </button>
        </header>
        <div className="sidebar-body">{children}</div>
      </aside>
    </div>
  );
};

export default Sidebar;
