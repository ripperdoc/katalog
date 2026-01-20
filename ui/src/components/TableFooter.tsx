import { FooterRendererProps } from "simple-table-core";

type TableFooterProps = FooterRendererProps & {
  queryTimeMs?: number | null;
};

const TableFooter = ({
  currentPage,
  startRow,
  endRow,
  totalRows,
  totalPages,
  rowsPerPage,
  hasPrevPage,
  hasNextPage,
  onPrevPage,
  onNextPage,
  onPageChange,
  queryTimeMs,
}: TableFooterProps) => {
  const isPrevDisabled = !hasPrevPage;
  const isNextDisabled = !hasNextPage;

  const handlePrevPage = async () => {
    const prevPage = Math.max(1, currentPage - 1);
    if (prevPage === currentPage) return;
    if (onPrevPage) {
      await onPrevPage();
    }
    onPageChange(prevPage);
  };

  const handleNextPage = async () => {
    const nextPage = currentPage + 1;
    if (onNextPage) {
      await onNextPage();
    }
    onPageChange(nextPage);
  };

  const handlePageChange = async (page: number) => {
    if (page >= 1 && page <= totalPages) {
      onPageChange(page);
    }
  };

  // Generate visible page numbers
  const getVisiblePages = () => {
    if (!Number.isFinite(totalPages) || totalPages <= 0) {
      return [currentPage];
    }
    // If there are 15 or fewer pages, show all
    if (totalPages <= 15) {
      return Array.from({ length: totalPages }, (_, i) => i + 1);
    }

    // Otherwise, show a window of pages with focus on the current page
    const pages = [];
    const maxDisplayed = 15; // Show maximum 15 page buttons

    // Calculate how to distribute the page numbers
    let startPage: number;
    let endPage: number;

    if (currentPage <= Math.ceil(maxDisplayed / 2)) {
      // Near the beginning - show first maxDisplayed-1 pages and the last page
      startPage = 1;
      endPage = maxDisplayed - 1;
    } else if (currentPage >= totalPages - Math.floor(maxDisplayed / 2)) {
      // Near the end - show last maxDisplayed pages
      startPage = Math.max(1, totalPages - maxDisplayed + 1);
      endPage = totalPages;
    } else {
      // In the middle - show a window around current page
      const pagesBeforeCurrent = Math.floor((maxDisplayed - 1) / 2);
      const pagesAfterCurrent = maxDisplayed - pagesBeforeCurrent - 1;
      startPage = currentPage - pagesBeforeCurrent;
      endPage = currentPage + pagesAfterCurrent;
    }

    // Add pages in the primary range
    for (let i = startPage; i <= endPage; i++) {
      pages.push(i);
    }

    // Add ellipsis and last page if not already included
    if (endPage < totalPages - 1) {
      pages.push(-1); // Ellipsis
      pages.push(totalPages);
    }

    return pages;
  };

  // Default footer
  const visiblePages = getVisiblePages();

  return (
    <div className="st-footer">
      <div className="st-footer-info">
        <span className="st-footer-results-text">
          Showing {startRow} to {endRow} of {totalRows.toLocaleString()} results
          {Number.isFinite(queryTimeMs ?? NaN) && (
            <span className="st-footer-duration" style={{ marginLeft: "0.5rem" }}>
              · query time {(queryTimeMs as number).toFixed(0)} ms
            </span>
          )}
        </span>
      </div>

      <div className="st-footer-pagination">
        {visiblePages.map((page, index) =>
          page < 0 ? (
            // Render ellipsis
            <span key={index} className="st-page-ellipsis">
              ...
            </span>
          ) : (
            // Render page button
            <button
              key={index}
              onClick={() => handlePageChange(page)}
              className={`st-page-btn ${currentPage === page ? "active" : ""}`}
              aria-label={`Go to page ${page}`}
              aria-current={currentPage === page ? "page" : undefined}
            >
              {page}
            </button>
          ),
        )}
        <button
          className={`st-next-prev-btn ${isPrevDisabled ? "disabled" : ""}`}
          onClick={handlePrevPage}
          disabled={isPrevDisabled}
          aria-label="Go to previous page"
        >
          <span className="icon" aria-hidden="true">
            chevron_left
          </span>
        </button>

        <button
          className={`st-next-prev-btn ${isNextDisabled ? "disabled" : ""}`}
          onClick={handleNextPage}
          disabled={isNextDisabled}
          aria-label="Go to next page"
        >
          <span className="icon" aria-hidden="true">
            chevron_right
          </span>
        </button>
      </div>
    </div>
  );
};

export default TableFooter;
