import os
import copy
import xml.etree.ElementTree as ET

NFL_TEAMS_PATH = "data/nfl_teams.xml"
TEAMS_PATH = "data/teams.xml"


def load_tree_and_root(path):
    if not os.path.exists(path):
        print(f"⚠️ {path} not found. Creating new file.")
        root = ET.Element("teams")
        ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)
    tree = ET.parse(path)
    return tree, tree.getroot()


def find_or_create_football_sport(root):
    for sport in root.findall("sport"):
        if sport.get("name") == "Football":
            return sport
    return ET.SubElement(root, "sport", {"name": "Football"})


def build_team_map(sport_node):
    return {
        team.findtext("name"): team
        for team in sport_node.findall("team")
    }


def elements_equal(a, b):
    return (a.tag == b.tag) and ((a.text or "").strip() == (b.text or "").strip())


def sync_team_fields(existing, source):
    modified = False
    for child in source:
        tag = child.tag
        existing_child = existing.find(tag)
        if existing_child is None:
            existing.append(copy.deepcopy(child))
            print(f"➕ Added missing <{tag}>")
            modified = True
        elif not elements_equal(existing_child, child):
            existing_child.text = child.text
            print(f"✏️ Updated <{tag}>")
            modified = True
    return modified


def main():
    nfl_tree, nfl_root = load_tree_and_root(NFL_TEAMS_PATH)
    teams_tree, teams_root = load_tree_and_root(TEAMS_PATH)

    football_node = find_or_create_football_sport(teams_root)
    existing_team_map = build_team_map(football_node)

    changes = 0

    for nfl_team in nfl_root.findall("team"):
        name = nfl_team.findtext("name")
        if not name:
            continue

        if name in existing_team_map:
            existing = existing_team_map[name]
            if sync_team_fields(existing, nfl_team):
                print(f"🔁 Updated team: {name}")
                changes += 1
        else:
            football_node.append(copy.deepcopy(nfl_team))
            print(f"➕ Added new team: {name}")
            changes += 1

    if changes > 0:
        teams_tree.write(TEAMS_PATH, encoding="utf-8", xml_declaration=True)
        print(f"\n✅ Wrote {changes} change(s) to {TEAMS_PATH}")
    else:
        print("\n✅ No changes needed.")


if __name__ == "__main__":
    main()
