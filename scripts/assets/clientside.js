if (!window.dccFunctions) {
  window.dccFunctions = {};
}
window.dccFunctions.transform_to_date = function (value) {
  const date = new Date(value * 1000);
  return (
    date.getFullYear() + "/" + (date.getMonth() + 1) + "/" + date.getDate()
  );
};
