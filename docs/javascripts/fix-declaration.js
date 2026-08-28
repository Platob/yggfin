// One reader for the declaration every FIX block ships.
//
// A component, a message and a repeating group are the same document in this
// repository -- a `Field`: a struct for a block, a list of one for a group,
// and an empty struct naming the block it defers to. Both pages that draw
// them read that one shape through here.
(() => {
  "use strict";

  const list = (value) => (Array.isArray(value) ? value : []);
  const object = (value) => (value && typeof value === "object" ? value : {});

  // The entry a block declares: a list declares it once and repeats it, a
  // struct is one already.
  const entryOf = (declared) =>
    object(declared).type === "list" ? object(object(declared).item) : object(declared);

  // A member that defers to a block declared elsewhere: it names the component
  // and carries none of its members, because expanding the published
  // dictionary in place turns three thousand members into a hundred thousand.
  const isReference = (member) => member.type === "struct" && !list(member.fields).length;

  // One block's members in wire order, groups nested under the group they
  // repeat inside.
  function members(declared) {
    return list(entryOf(declared).fields).map((member) => {
      const group = member.type === "list";
      const tag = object(member.fix).tag;
      return {
        kind: group ? "group" : isReference(member) ? "component" : "field",
        name: member.name,
        tag: tag === undefined ? undefined : Number(tag),
        // `nullable` is the stored spelling of optional, and a declaration
        // writes it only when it is true.
        required: member.nullable !== true,
        members: group ? members(member) : [],
      };
    });
  }

  // The message type a declaration defines, or nothing for a plain component.
  const msgType = (declared) => object(object(declared).fix).msgtype || "";

  window.fixDeclaration = { members, msgType };
})();
