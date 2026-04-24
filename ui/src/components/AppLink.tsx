import type { MouseEvent, ReactNode } from "react";
import { getAppHref, navigateInApp } from "../utils/appNavigation";
import { stopTableCellSelection } from "../utils/tableCellLinkEvents";

const shouldHandleClientSide = (event: MouseEvent<HTMLAnchorElement>): boolean => {
  return !(
    event.defaultPrevented ||
    event.button !== 0 ||
    event.metaKey ||
    event.ctrlKey ||
    event.shiftKey ||
    event.altKey
  );
};

type AppLinkProps = {
  to: string;
  children: ReactNode;
  className?: string;
  title?: string;
  onClick?: (event: MouseEvent<HTMLAnchorElement>) => void;
};

function AppLink({ to, children, className, title, onClick }: AppLinkProps) {
  const href = getAppHref(to);

  return (
    <a
      href={href}
      className={className}
      title={title}
      onPointerDown={stopTableCellSelection}
      onMouseDown={stopTableCellSelection}
      onTouchStart={stopTableCellSelection}
      onClick={(event) => {
        onClick?.(event);
        if (!shouldHandleClientSide(event)) {
          return;
        }
        event.preventDefault();
        event.stopPropagation();
        navigateInApp(to);
      }}
    >
      {children}
    </a>
  );
}

export default AppLink;
