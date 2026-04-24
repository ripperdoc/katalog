export const stopTableCellSelection = (event: { stopPropagation: () => void }) => {
  event.stopPropagation();
};
