export const COLUMNS = [
  { key: "_id",       header: "ID" },
  { key: "fullName",      header: "Name" },
  { key: "email",     header: "Email" },
  { key: "mobile",     header: "Phone" },
  { key: "createdAt", header: "Created At" },
  {key: "grade" , header: "Grade"} ,
  {key: "subject" , header: "Subject"}
];

export function docToRow(doc) {
  return COLUMNS.map(({ key }) => {
    const v = doc?.[key];
    if (v == null) return "";
    if (v instanceof Date) return v.toISOString();
    if (typeof v === "object") return v.toString?.() ?? JSON.stringify(v);
    return v;
  });
}