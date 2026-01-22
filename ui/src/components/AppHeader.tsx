import { ReactNode } from "react";
import { Link } from "react-router-dom";
import ChangesetProgressBar from "./ChangesetProgressBar";

interface AppHeaderProps {
  children?: ReactNode;
}

const AppHeader = ({ children }: AppHeaderProps) => {
  return (
    <header className="app-header">
      <div className="toolbar">
        <h1>
          Katalog <small>{__APP_VERSION__ ?? "dev"}</small> <span className="icon">view_list</span>
        </h1>
        <nav className="nav">
          <Link to="/actors">Actors</Link>
          <Link to="/assets">Assets</Link>
          <Link to="/collections">Collections</Link>
          <Link to="/changesets">History</Link>
        </nav>
      </div>
      <ChangesetProgressBar />
      {children && <div className="page-header">{children}</div>}
    </header>
  );
};

export default AppHeader;
