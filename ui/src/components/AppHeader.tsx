import { ReactNode, useMemo } from "react";
import { Link, useLocation } from "react-router-dom";
import ChangesetProgressBar from "./ChangesetProgressBar";

interface AppHeaderProps {
  children?: ReactNode;
  breadcrumbLabel?: string | null;
}

type BreadcrumbItem = {
  label: string;
  to: string;
};

const AppHeader = ({ children, breadcrumbLabel }: AppHeaderProps) => {
  const location = useLocation();
  const breadcrumbs = useMemo(() => {
    const segments = location.pathname.split("/").filter(Boolean);
    if (segments.length === 0) {
      return [];
    }

    const topLevel = segments[0];
    const topMap: Record<string, string> = {
      actors: "Actors",
      assets: "Assets",
      collections: "Collections",
      changesets: "History",
    };
    const topLabel = topMap[topLevel];
    if (!topLabel) {
      return [];
    }

    const items: BreadcrumbItem[] = [{ label: topLabel, to: `/${topLevel}` }];
    if (segments.length < 2) {
      return items;
    }

    const second = segments[1];
    if (topLevel === "actors" && second === "new") {
      items.push({ label: "New Actor", to: `/${topLevel}/${second}` });
      return items;
    }

    const defaultLabelMap: Record<string, string> = {
      actors: `Actor ${second}`,
      assets: `Asset ${second}`,
      collections: `Collection ${second}`,
      changesets: `Changeset ${second}`,
    };
    const leafLabel = breadcrumbLabel?.trim() || defaultLabelMap[topLevel] || second;
    items.push({ label: leafLabel, to: `/${topLevel}/${second}` });
    return items;
  }, [location.pathname, breadcrumbLabel]);

  return (
    <header className="app-header">
      <div className="toolbar">
        <h1>
          <span className="icon">view_list</span>
          <span className="app-title">Katalog</span>
          {breadcrumbs.length > 0 && (
            <span className="app-breadcrumbs">
              <span className="breadcrumb-separator">/</span>
              {breadcrumbs.map((crumb, index) => (
                <span key={`${crumb.to}-${index}`} className="breadcrumb-item">
                  <Link to={crumb.to}>{crumb.label}</Link>
                  {index < breadcrumbs.length - 1 && (
                    <span className="breadcrumb-separator">/</span>
                  )}
                </span>
              ))}
            </span>
          )}
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
