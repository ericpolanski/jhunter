# Design Specification: JHunter

## Overview

JHunter is a personal job hunting application for Eric Polanski, a recent Northwestern CS graduate seeking AI Engineering and Software Engineering roles in the Chicagoland area. The application provides job scraping, AI-powered fit scoring, resume tailoring, cover letter generation, application tracking, and analytics.

---

## Visual Design

### Color Palette

| Role | Color | Hex | Usage |
|------|-------|-----|-------|
| Primary | Deep Teal | `#0D7377` | Primary actions, headers, active states |
| Primary Dark | Dark Teal | `#095456` | Hover states, emphasis |
| Secondary | Warm Coral | `#E85A4F` | CTAs, alerts, important notifications |
| Accent | Golden Amber | `#F5A623` | Fit scores (high), badges, highlights |
| Background Primary | Off-White | `#FAFBFC` | Main page backgrounds |
| Background Secondary | Warm Gray | `#F3F4F6` | Cards, sidebar, sections |
| Surface | White | `#FFFFFF` | Card surfaces, modals |
| Text Primary | Charcoal | `#1F2937` | Headings, body text |
| Text Secondary | Slate | `#6B7280` | Labels, secondary info |
| Text Muted | Light Gray | `#9CA3AF` | Placeholders, disabled |
| Border | Soft Gray | `#E5E7EB` | Card borders, dividers |
| Success | Green | `#10B981` | Application status: offer |
| Warning | Amber | `#F59E0B` | Reminders, follow-ups |
| Error | Red | `#EF4444` | Errors, rejections |

### Fit Score Color Scale

| Score Range | Color | Hex |
|-------------|-------|-----|
| 80-100 | Emerald | `#10B981` |
| 60-79 | Teal | `#0D7377` |
| 40-59 | Amber | `#F5A623` |
| 20-39 | Orange | `#F97316` |
| 0-19 | Coral | `#E85A4F` |

### Typography

| Element | Font | Weight | Size | Line Height |
|---------|------|--------|------|-------------|
| Logo/Brand | Playfair Display | 700 | 28px | 1.2 |
| H1 | DM Sans | 700 | 32px | 1.3 |
| H2 | DM Sans | 600 | 24px | 1.35 |
| H3 | DM Sans | 600 | 20px | 1.4 |
| H4 | DM Sans | 500 | 16px | 1.5 |
| Body | DM Sans | 400 | 15px | 1.6 |
| Small | DM Sans | 400 | 13px | 1.5 |
| Label | DM Sans | 500 | 12px | 1.4 |
| Monospace | JetBrains Mono | 400 | 13px | 1.5 |

### Spacing System

Base unit: 4px

| Token | Value | Usage |
|-------|-------|-------|
| xs | 4px | Tight spacing, icon gaps |
| sm | 8px | Inline elements, small gaps |
| md | 16px | Standard padding, gaps |
| lg | 24px | Section spacing |
| xl | 32px | Major section gaps |
| 2xl | 48px | Page margins |
| 3xl | 64px | Hero spacing |

### Border Radius

| Token | Value | Usage |
|-------|-------|-------|
| sm | 4px | Buttons, inputs |
| md | 8px | Cards, panels |
| lg | 12px | Modals, large cards |
| full | 9999px | Pills, avatars |

### Shadows

| Token | Usage | CSS |
|-------|-------|-----|
| sm | Subtle elevation | `0 1px 2px rgba(0,0,0,0.05)` |
| md | Cards, dropdowns | `0 4px 6px -1px rgba(0,0,0,0.1)` |
| lg | Modals, toasts | `0 10px 15px -3px rgba(0,0,0,0.1)` |
| xl | Floating elements | `0 20px 25px -5px rgba(0,0,0,0.1)` |

### Visual Effects

- **Card hover**: Subtle lift with `transform: translateY(-2px)` and shadow increase
- **Focus states**: 2px teal outline with 2px offset
- **Loading states**: Skeleton screens with shimmer animation (left-to-right gradient sweep)
- **Transitions**: All interactive elements use `transition: all 0.2s ease-out`

---

## Component Library

### Layout Components

#### Sidebar
- Fixed left sidebar (240px width on desktop)
- Logo at top
- Navigation items with icons
- Active state: teal background (`#0D7377` at 10% opacity), teal left border
- Hover: subtle background shift
- Collapsible on tablet (icon-only mode, 64px)
- Hidden on mobile (hamburger menu)

#### Header
- Sticky top header (64px height)
- Page title (H2)
- Action buttons (right-aligned)
- Search bar (expandable on mobile)

#### Page Container
- Max-width: 1400px centered
- Padding: 32px (desktop), 16px (mobile)
- Background: `#FAFBFC`

### Navigation Components

#### NavItem
- Icon (20px) + Label
- States: default, hover, active, disabled
- Tooltip on collapsed sidebar

#### Breadcrumb
- Separator: `/`
- Current page not linked
- Truncate with ellipsis if > 4 segments

### Data Display Components

#### StatCard
- Icon + Label (small, muted)
- Value (large, bold)
- Trend indicator (up/down arrow with percentage)
- Background: white with subtle shadow

#### JobCard
- Company logo (40px, rounded)
- Job title (bold)
- Company name + location
- Fit score badge (colored pill)
- Posted date (relative: "2h ago", "3d ago")
- Source badge (LinkedIn, Indeed, Glassdoor)
- Hover: lift effect + border color change

#### FitScoreBadge
- Circular badge (48px)
- Score number centered
- Color based on score scale
- Breakdown tooltip on hover

#### StatusBadge
- Pill-shaped
- Color-coded by status:
  - saved: `#6B7280` (gray)
  - applied: `#0D7377` (teal)
  - phone_screen: `#F5A623` (amber)
  - interview: `#8B5CF6` (purple)
  - offer: `#10B981` (green)
  - rejected: `#EF4444` (red)
  - withdrawn: `#9CA3AF` (light gray)

#### Table
- Sticky header
- Alternating row colors (subtle)
- Sortable columns with indicator
- Row hover highlight
- Pagination at bottom

#### KanbanBoard
- Columns: saved, applied, phone_screen, interview, offer, rejected
- Drag-and-drop between columns
- Card count per column
- Add card button per column

### Form Components

#### Input
- Height: 44px
- Border: 1px `#E5E7EB`
- Focus: teal border + shadow
- Error: red border + error message below
- Label above (12px, semi-bold)

#### Select
- Custom dropdown styling
- Search/filter for long lists
- Multi-select variant with tags

#### Button
- Primary: teal background, white text
- Secondary: white background, teal border + text
- Danger: coral background, white text
- Ghost: transparent, text only
- Sizes: sm (32px), md (40px), lg (48px)
- Loading state with spinner

#### Checkbox/Radio
- Custom styled (not browser default)
- Teal check/fill
- Label to the right

#### Toggle
- Pill-shaped track
- Teal when active

### Feedback Components

#### Toast/Alert
- Position: top-right
- Types: success, warning, error, info
- Auto-dismiss after 5s (configurable)
- Close button
- Stacked if multiple

#### Modal
- Centered, max-width 600px (configurable)
- Backdrop: `rgba(0,0,0,0.5)` with blur
- Close button top-right
- Footer with action buttons

#### Skeleton
- Shimmer animation
- Match layout of content being loaded

#### EmptyState
- Icon (48px, muted color)
- Title + description
- Optional CTA button

### Chart Components

#### ChartContainer
- White background, subtle shadow
- Title + description
- Legend below or right
- Responsive sizing

#### Chart Types Used
- Line chart: application timeline
- Bar chart: applications by source
- Pie/Donut: status breakdown
- Area: fit score distribution

---

## Page Layouts

### Dashboard (`/`)

```
┌─────────────────────────────────────────────────────────────┐
│ HEADER: "Dashboard"                    [+ New Job] [Sync]  │
├─────────────────────────────────────────────────────────────┤
│ STATS ROW                                                  │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│ │ Applied  │ │ Response │ │ Interview│ │ Offers   │        │
│ │    12    │ │   25%    │ │    3     │ │    0     │        │
│ └──────────┘ └──────────┘ └──────────┘ └──────────┘        │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────┐ ┌─────────────────────────────┐  │
│ │ PIPELINE OVERVIEW      │ │ REMINDERS                  │  │
│ │ [Kanban mini-view]     │ │ - Follow-up due (2)        │  │
│ │                        │ │ - No response (5)          │  │
│ └─────────────────────────┘ └─────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ TOP NEW JOBS                                           │ │
│ │ [JobCard] [JobCard] [JobCard] [JobCard]                │ │
│ │ See All →                                              │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Job Board (`/jobs`)

```
┌─────────────────────────────────────────────────────────────┐
│ HEADER: "Job Board"                    [+ Add Job] [Sync] │
├─────────────────────────────────────────────────────────────┤
│ FILTERS BAR                                                │
│ [Source ▼] [Location ▼] [Min Score ▼] [Sort ▼] [Search___]│
├─────────────────────────────────────────────────────────────┤
│ RESULTS: 47 jobs                                           │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ [JobCard]                                             │ │
│ │ ▸ Senior AI Engineer • AbbVie • Chicago, IL    85 ▸  │ │
│ └─────────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ [JobCard]                                             │ │
│ │ ▸ ML Engineer • Capital One • Chicago, IL       78 ▸  │ │
│ └─────────────────────────────────────────────────────────┘ │
│ ...                                                        │
├─────────────────────────────────────────────────────────────┤
│ PAGINATION: [< Prev] Page 1 of 5 [Next >]                  │
└─────────────────────────────────────────────────────────────┘
```

### Job Detail (`/jobs/:id`)

```
┌─────────────────────────────────────────────────────────────┐
│ [< Back]                                    [Save] [Apply] │
├─────────────────────────────────────────────────────────────┤
│ ┌───────────────────────────────┐ ┌───────────────────────┐ │
│ │ JOB HEADER                   │ │ FIT SCORE            │ │
│ │ Title                        │ │   ┌─────┐            │ │
│ │ Company • Location           │ │   │  85 │            │ │
│ │ [Apply Link] [Source]        │ │   └─────┘            │ │
│ └───────────────────────────────┘ └───────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ TABS: [Details] [Resume] [Cover Letter] [Company] [Prep]   │
├─────────────────────────────────────────────────────────────┤
│ TAB CONTENT                                                │
│ - Full job description                                     │
│ - Requirements list                                        │
│ - Salary information                                       │
│ - Posted date                                              │
└─────────────────────────────────────────────────────────────┘
```

### Applications (`/applications`)

```
┌─────────────────────────────────────────────────────────────┐
│ HEADER: "Applications"           [View: Table | Kanban]     │
├─────────────────────────────────────────────────────────────┤
│ ┌────────┐ ┌────────┐ ┌───────────┐ ┌──────────┐ ┌───────┐ │
│ │ Saved  │ │ Applied│ │Phone Screen│ │Interview│ │ Offer │ │
│ │  (12)  │ │  (15)  │ │    (3)    │ │   (4)   │ │  (0)  │ │
│ └────────┘ └────────┘ └───────────┘ └──────────┘ └───────┘ │
├─────────────────────────────────────────────────────────────┤
│ TABLE VIEW:                                                │
│ ┌────────────────────────────────────────────────────────┐ │
│ │ Job │ Company │ Score │ Status │ Applied │ Notes │ ⋮ │ │
│ ├─────┼─────────┼───────┼────────┼─────────┼───────┼───│ │
│ │ ... │ ...     │ ...   │ ...    │ ...     │ ...   │ ⋮ │ │
│ └────────────────────────────────────────────────────────┘ │
│                                                         or  │
│ KANBAN VIEW:                                             │
│ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐               │
│ │Saved  │ │Applied │ │Phone   │ │Interview│              │
│ │       │ │        │ │Screen  │ │        │               │
│ │[Card] │ │[Card]  │ │[Card]  │ │[Card]  │               │
│ │[Card] │ │[Card]  │ │[Card]  │ │[Card]  │               │
│ └────────┘ └────────┘ └────────┘ └────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### Analytics (`/analytics`)

```
┌─────────────────────────────────────────────────────────────┐
│ HEADER: "Analytics"                    [Date Range ▼]     │
├─────────────────────────────────────────────────────────────┤
│ ┌──────────────────────────┐ ┌──────────────────────────┐  │
│ │ APPLICATION FUNNEL        │ │ FIT SCORE DISTRIBUTION   │  │
│ │ [Funnel Chart]           │ │ [Bar Chart]              │  │
│ └──────────────────────────┘ └──────────────────────────┘  │
│ ┌──────────────────────────┐ ┌──────────────────────────┐  │
│ │ RESPONSE BY SOURCE       │ │ SALARY DISTRIBUTION      │  │
│ │ [Bar Chart]              │ │ [Area Chart]             │  │
│ └──────────────────────────┘ └──────────────────────────┘  │
│ ┌──────────────────────────┐ ┌──────────────────────────┐  │
│ │ APPLICATION TIMELINE     │ │ SKILL GAP ANALYSIS        │  │
│ │ [Line Chart]             │ │ [Radar Chart]            │  │
│ └──────────────────────────┘ └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Settings (`/settings`)

```
┌─────────────────────────────────────────────────────────────┐
│ HEADER: "Settings"                                         │
├─────────────────────────────────────────────────────────────┤
│ ┌──────────────────────────┐ ┌──────────────────────────┐  │
│ │ SCRAPING CONFIG         │ │ REMINDER SETTINGS        │  │
│ │ Sources: [x] LinkedIn  │ │ Follow-up: [7] days      │  │
│ │         [x] Indeed      │ │ No response: [14] days   │  │
│ │         [x] Glassdoor   │ │                          │  │
│ │ Location: [Chicago, IL] │ │                          │  │
│ └──────────────────────────┘ └──────────────────────────┘  │
│ ┌──────────────────────────────────────────────────────────┐│
│ │ PROFILE EDITOR                                           ││
│ │ [Edit Eric's resume/profile data]                        ││
│ └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Responsive Breakpoints

| Breakpoint | Width | Layout Changes |
|------------|-------|----------------|
| Mobile | < 640px | Single column, hidden sidebar (hamburger), stacked cards |
| Tablet | 640-1024px | Collapsed sidebar (icons), 2-column grids |
| Desktop | > 1024px | Full sidebar, 3-4 column grids |

### Mobile Adaptations

- Sidebar becomes slide-out drawer
- Cards stack vertically
- Tables become scrollable or card view
- Charts resize to container width
- Bottom navigation bar (optional)

### Tablet Adaptations

- Sidebar collapses to icon-only (64px)
- Main content area expands
- 2-column grid for stats and charts

---

## Interaction Patterns

### Navigation
- Active page indicated in sidebar
- Breadcrumb for deep pages
- Smooth scroll to top on page change

### Job Cards
- Click card → Job Detail page
- Hover → subtle lift + shadow
- Quick actions on hover (save, apply)

### Application Status Changes
- Click status badge → dropdown to change
- Drag-and-drop in Kanban view
- Confirmation toast on status change

### Fit Score
- Click badge → modal with full breakdown
- Tooltip shows quick breakdown on hover

### Forms
- Inline validation on blur
- Submit button disabled until valid
- Loading state during submission

### Drag and Drop (Kanban)
- Visual feedback on drag (card lifts, shadow)
- Drop zones highlight
- Smooth animation on drop

### Modals
- Close on backdrop click
- Close on Escape key
- Focus trap inside modal

### Filters
- Filters persist in URL params
- Clear all filters button
- Results update live (debounced 300ms)

---

## Animations

### Page Transitions
- Fade in: 200ms ease-out

### Card Interactions
- Hover lift: `transform: translateY(-2px)` over 200ms
- Click scale: `transform: scale(0.98)` quick pulse

### Loading States
- Skeleton shimmer: 1.5s infinite
- Spinner: rotate 1s linear infinite

### Status Changes (Kanban)
- Card flies to new column: 300ms ease-out

### Toast Notifications
- Slide in from right: 300ms
- Slide out: 200ms

### Charts
- Draw animation: 800ms ease-out
- Staggered entry for data points

---

## Accessibility

- All interactive elements keyboard accessible
- Focus indicators visible (teal outline)
- Color contrast meets WCAG AA (4.5:1 for text)
- ARIA labels on icons and buttons
- Screen reader friendly table headers

---

## Design Tokens Summary

```css
/* Colors */
--color-primary: #0D7377;
--color-primary-dark: #095456;
--color-secondary: #E85A4F;
--color-accent: #F5A623;
--color-bg-primary: #FAFBFC;
--color-bg-secondary: #F3F4F6;
--color-surface: #FFFFFF;
--color-text-primary: #1F2937;
--color-text-secondary: #6B7280;
--color-text-muted: #9CA3AF;
--color-border: #E5E7EB;

/* Spacing */
--space-xs: 4px;
--space-sm: 8px;
--space-md: 16px;
--space-lg: 24px;
--space-xl: 32px;
--space-2xl: 48px;
--space-3xl: 64px;

/* Typography */
--font-display: 'Playfair Display', serif;
--font-sans: 'DM Sans', sans-serif;
--font-mono: 'JetBrains Mono', monospace;

/* Shadows */
--shadow-sm: 0 1px 2px rgba(0,0,0,0.05);
--shadow-md: 0 4px 6px -1px rgba(0,0,0,0.1);
--shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.1);

/* Border Radius */
--radius-sm: 4px;
--radius-md: 8px;
--radius-lg: 12px;
--radius-full: 9999px;
```

---

## Implementation Notes

1. **Icons**: Use Lucide React for consistent iconography
2. **Charts**: Recharts library as specified in SPEC.md
3. **Animations**: Framer Motion for complex animations, CSS for simple transitions
4. **Tailwind**: Use custom theme config extending default Tailwind tokens
5. **Fonts**: Load via Google Fonts CDN or self-host for offline support
