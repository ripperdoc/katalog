import { ReactNode } from "react";
import { Link } from "react-router-dom";

interface AppHeaderProps {
  children?: ReactNode;
}

const AppHeader = ({ children }: AppHeaderProps) => {
  return (
    <header className="app-header">
      <div className="toolbar">
        <h1>
          Katalog <small>{__APP_VERSION__ ?? "dev"}</small>
        </h1>
        <nav className="nav">
          <Link to="/providers">Providers</Link>
          <Link to="/assets">Assets</Link>
          <Link to="/collections">Collections</Link>
          <Link to="/snapshots">History</Link>
        </nav>
      </div>
      {children && <div className="page-header">{children}</div>}
    </header>
  );
};

export default AppHeader;
